import { describe, expect, it } from "bun:test";
import { PollingLoop } from "../../../src/application/polling-loop";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

describe("PollingLoop", () => {
  it("executa o trabalho repetidamente e para com stop()", async () => {
    let runs = 0;
    const loop = new PollingLoop({ name: "t", work: async () => (runs++, false), idleDelayMs: 5, onError: () => {} });
    loop.start();
    await sleep(80);
    await loop.stop();

    const stopped = runs;
    expect(stopped).toBeGreaterThan(3);
    await sleep(40);
    expect(runs).toBe(stopped);
  });

  it("não espera entre iterações enquanto há trabalho", async () => {
    let remaining = 50;
    let runs = 0;
    const loop = new PollingLoop({
      name: "t",
      work: async () => {
        runs++;
        return remaining-- > 0;
      },
      idleDelayMs: 5_000,
      onError: () => {},
    });
    const started = Date.now();
    loop.start();
    while (runs < 51 && Date.now() - started < 2_000) await sleep(5);
    await loop.stop();

    expect(runs).toBeGreaterThanOrEqual(51);
    expect(Date.now() - started).toBeLessThan(1_500);
  });

  it("stop() aguarda a iteração em andamento terminar", async () => {
    let finished = false;
    const loop = new PollingLoop({
      name: "t",
      work: async () => {
        await sleep(120);
        finished = true;
        return false;
      },
      idleDelayMs: 5,
      onError: () => {},
    });
    loop.start();
    await sleep(20);
    await loop.stop();
    expect(finished).toBe(true);
  });

  it("stop() interrompe a espera ociosa imediatamente", async () => {
    const loop = new PollingLoop({ name: "t", work: async () => false, idleDelayMs: 10_000, onError: () => {} });
    loop.start();
    await sleep(30);
    const started = Date.now();
    await loop.stop();
    expect(Date.now() - started).toBeLessThan(300);
  });

  it("erros são reportados e o loop continua", async () => {
    const errors: unknown[] = [];
    let calls = 0;
    const loop = new PollingLoop({
      name: "t",
      work: async () => {
        calls++;
        if (calls <= 2) throw new Error(`falha ${calls}`);
        return false;
      },
      idleDelayMs: 5,
      errorDelayMs: 5,
      onError: (e) => errors.push(e),
    });
    loop.start();
    await sleep(100);
    await loop.stop();

    expect(errors).toHaveLength(2);
    expect(calls).toBeGreaterThan(3);
  });

  it("start() duas vezes não cria dois loops", async () => {
    let concurrent = 0;
    let maxConcurrent = 0;
    const loop = new PollingLoop({
      name: "t",
      work: async () => {
        maxConcurrent = Math.max(maxConcurrent, ++concurrent);
        await sleep(10);
        concurrent--;
        return false;
      },
      idleDelayMs: 1,
      onError: () => {},
    });
    loop.start();
    loop.start();
    await sleep(60);
    await loop.stop();
    expect(maxConcurrent).toBe(1);
  });

  it("start() logo depois de stop() não deixa o laço antigo vivo nem cria dois laços", async () => {
    let concurrent = 0;
    let maxConcurrent = 0;
    let runs = 0;
    const loop = new PollingLoop({
      name: "t",
      work: async () => {
        runs++;
        maxConcurrent = Math.max(maxConcurrent, ++concurrent);
        await sleep(30);
        concurrent--;
        return false;
      },
      idleDelayMs: 1,
      onError: () => {},
    });
    loop.start();
    await sleep(10); // a iteração do primeiro laço está em andamento
    const stopping = loop.stop();
    loop.start(); // reinício antes de o laço antigo terminar
    await stopping;
    await sleep(100);
    expect(maxConcurrent).toBe(1);

    await loop.stop();
    const stopped = runs;
    await sleep(60);
    expect(runs).toBe(stopped); // o stop() final realmente parou tudo (a referência do laço novo não foi perdida)
  });
});
