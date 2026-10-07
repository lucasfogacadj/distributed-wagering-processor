import type { Clock, IdGenerator } from "../application/ports";

export class SystemClock implements Clock {
  now(): Date {
    return new Date();
  }
}

/** UUID v7: ordenável no tempo, o que mantém os índices primários compactos. */
export class UuidV7Generator implements IdGenerator {
  next(): string {
    return Bun.randomUUIDv7();
  }
}
