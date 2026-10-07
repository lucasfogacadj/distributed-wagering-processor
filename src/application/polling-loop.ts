export interface PollingLoopOptions {
  name: string;
  /** Uma iteração. Devolve true se havia trabalho (a próxima roda sem pausa) e false se estava ocioso. */
  work: () => Promise<boolean>;
  idleDelayMs: number;
  /** Pausa depois de uma iteração que lançou erro (padrão: idleDelayMs). */
  errorDelayMs?: number;
  onError: (error: unknown) => void;
}

/** Laço de polling com parada graciosa: `stop()` interrompe a espera e aguarda a iteração em andamento. */
export class PollingLoop {
  private running = false;
  /** Identifica o laço vigente: um laço antigo que ainda está terminando a iteração sai ao ver outra geração. */
  private generation = 0;
  private loop: Promise<void> | undefined;
  private timer: ReturnType<typeof setTimeout> | undefined;
  private wake: (() => void) | undefined;

  constructor(private readonly options: PollingLoopOptions) {}

  get name(): string {
    return this.options.name;
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    const generation = ++this.generation;
    const previous = this.loop;
    // Um start() logo depois de stop() espera o laço anterior sair (ele sai pela troca de geração, e não pelo
    // `running`, que já voltou a true): nunca há dois laços executando `work` ao mesmo tempo.
    this.loop = (async () => {
      await previous?.catch(() => undefined);
      await this.run(generation);
    })();
  }

  async stop(): Promise<void> {
    this.running = false;
    this.interrupt();
    const loop = this.loop;
    await loop;
    // Só limpa se ninguém iniciou um laço novo enquanto esperávamos.
    if (this.loop === loop) this.loop = undefined;
  }

  private async run(generation: number): Promise<void> {
    while (this.running && generation === this.generation) {
      let delayMs: number;
      try {
        const didWork = await this.options.work();
        delayMs = didWork ? 0 : this.options.idleDelayMs;
      } catch (error) {
        this.options.onError(error);
        delayMs = this.options.errorDelayMs ?? this.options.idleDelayMs;
      }

      if (!this.running || generation !== this.generation) break;
      // Com trabalho contínuo, devolve o controle ao event loop entre iterações.
      await (delayMs > 0 ? this.sleep(delayMs) : new Promise<void>((resolve) => setImmediate(resolve)));
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise<void>((resolve) => {
      this.wake = resolve;
      this.timer = setTimeout(() => {
        this.wake = undefined;
        resolve();
      }, ms);
    });
  }

  private interrupt(): void {
    if (this.timer) clearTimeout(this.timer);
    this.timer = undefined;
    this.wake?.();
    this.wake = undefined;
  }
}
