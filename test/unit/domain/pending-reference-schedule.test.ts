import { describe, expect, it } from "bun:test";
import { PENDING_REFERENCE_MAX_ATTEMPTS, pendingReferenceDelayMs } from "../../../src/domain/pending-reference-schedule";

describe("pendingReferenceDelayMs", () => {
  it("dobra a cada tentativa a partir de 1s", () => {
    expect([0, 1, 2, 3].map(pendingReferenceDelayMs)).toEqual([1_000, 2_000, 4_000, 8_000]);
  });

  it("tem teto de 5 minutos", () => {
    expect(pendingReferenceDelayMs(20)).toBe(300_000);
    expect(pendingReferenceDelayMs(1_000)).toBe(300_000);
  });

  it("o limite de tentativas é finito e documentado", () => {
    expect(PENDING_REFERENCE_MAX_ATTEMPTS).toBe(10);
  });
});
