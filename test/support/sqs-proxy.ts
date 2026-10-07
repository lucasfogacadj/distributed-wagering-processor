import { LOCALSTACK_ENDPOINT } from "./sqs";

/**
 * Quando reter a requisição:
 *  - "before": antes de repassá-la ao SQS (o broker nunca a vê);
 *  - "after":  depois que o SQS a executou, sem devolver a resposta ao cliente.
 * Em ambos os casos o cliente fica esperando — é a janela em que o teste dá SIGKILL no processo.
 */
export type StallMode = "before" | "after";

const HOP_BY_HOP = new Set(["host", "connection", "content-length", "content-encoding", "transfer-encoding", "keep-alive"]);

function actionOf(request: Request, body: ArrayBuffer | undefined): string | undefined {
  const target = request.headers.get("x-amz-target"); // protocolo JSON do SQS: "AmazonSQS.DeleteMessage"
  if (target) return target.split(".").pop();
  if (body) return /(?:^|&)Action=(\w+)/.exec(new TextDecoder().decode(body))?.[1]; // protocolo query
  return undefined;
}

/** Número de mensagens de uma requisição: as entradas de um `*Batch` (protocolo JSON), ou 1 para ações unitárias. */
function entriesOf(action: string, body: ArrayBuffer | undefined): number {
  if (!action.endsWith("Batch") || !body) return 1;
  try {
    const parsed = JSON.parse(new TextDecoder().decode(body)) as { Entries?: unknown[] };
    return Array.isArray(parsed.Entries) ? parsed.Entries.length : 0;
  } catch {
    return 0;
  }
}

/**
 * Proxy HTTP na frente do SQS, para provocar falhas deterministas SEM ganchos de teste no código de produção:
 * a instância sob teste aponta para o proxy, que segura uma ação específica enquanto o teste a mata.
 */
export class SqsProxy {
  private readonly stalls = new Map<string, StallMode>();
  private readonly forwarded = new Map<string, number>();
  private readonly forwardedEntries = new Map<string, number>();
  private readonly stalled = new Map<string, number>();
  private readonly waiters: { action: string; resolve: () => void }[] = [];

  private constructor(
    private readonly server: ReturnType<typeof Bun.serve>,
    readonly url: string,
  ) {}

  static start(target: string = LOCALSTACK_ENDPOINT): SqsProxy {
    // A rota é registrada antes de a instância existir; `self` é preenchido logo abaixo.
    let self!: SqsProxy;
    const server = Bun.serve({
      port: 0,
      hostname: "127.0.0.1",
      idleTimeout: 0,
      fetch: (request) => self.handle(request, target),
    });
    self = new SqsProxy(server, `http://127.0.0.1:${server.port}`);
    return self;
  }

  /** Passa a reter as requisições desta ação (ex.: "DeleteMessage", "SendMessage"). */
  stall(action: string, mode: StallMode = "before"): void {
    this.stalls.set(action, mode);
  }

  clearStalls(): void {
    this.stalls.clear();
  }

  /** Quantas requisições desta ação foram repassadas ao SQS (inclui as retidas em modo "after"). */
  count(action: string): number {
    return this.forwarded.get(action) ?? 0;
  }

  /**
   * Quantas MENSAGENS foram repassadas ao SQS por ações de lote (`SendMessageBatch`): cada entrada conta uma.
   * É o que prova "cada evento saiu exatamente uma vez" quando o relay publica em lotes.
   */
  entries(action: string): number {
    return this.forwardedEntries.get(action) ?? 0;
  }

  /** Resolve quando alguma requisição desta ação ficar retida. */
  waitForStall(action: string, timeoutMs = 30_000): Promise<void> {
    if ((this.stalled.get(action) ?? 0) > 0) return Promise.resolve();
    return new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`Nenhuma requisição ${action} foi retida em ${timeoutMs}ms`)), timeoutMs);
      this.waiters.push({
        action,
        resolve: () => {
          clearTimeout(timer);
          resolve();
        },
      });
    });
  }

  stop(): void {
    this.server.stop(true);
  }

  private noteStall(action: string): void {
    this.stalled.set(action, (this.stalled.get(action) ?? 0) + 1);
    for (const waiter of this.waiters.filter((w) => w.action === action)) waiter.resolve();
  }

  private async handle(request: Request, target: string): Promise<Response> {
    const url = new URL(request.url);
    const body = request.method === "GET" || request.method === "HEAD" ? undefined : await request.arrayBuffer();
    const action = actionOf(request, body);
    const mode = action ? this.stalls.get(action) : undefined;

    if (mode === "before" && action) {
      this.noteStall(action);
      await new Promise<never>(() => undefined);
    }

    const headers = new Headers();
    request.headers.forEach((value, key) => {
      if (!HOP_BY_HOP.has(key.toLowerCase())) headers.set(key, value);
    });
    const upstream = await fetch(`${target}${url.pathname}${url.search}`, {
      method: request.method,
      headers,
      ...(body !== undefined ? { body } : {}),
    });
    const payload = await upstream.arrayBuffer();
    if (action) {
      this.forwarded.set(action, (this.forwarded.get(action) ?? 0) + 1);
      this.forwardedEntries.set(action, (this.forwardedEntries.get(action) ?? 0) + entriesOf(action, body));
    }

    if (mode === "after" && action) {
      this.noteStall(action);
      await new Promise<never>(() => undefined);
    }

    const responseHeaders = new Headers();
    upstream.headers.forEach((value, key) => {
      if (!HOP_BY_HOP.has(key.toLowerCase())) responseHeaders.set(key, value);
    });
    return new Response(payload, { status: upstream.status, headers: responseHeaders });
  }
}
