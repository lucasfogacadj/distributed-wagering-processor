import { InvalidQueryError } from "./errors";

const MAX_BIGINT = 9_223_372_036_854_775_807n;

/** Cursor opaco: base64url de { v, s } onde `s` é o seq (identity) do último lançamento devolvido. */
export function encodeCursor(seq: string): string {
  return Buffer.from(JSON.stringify({ v: 1, s: seq })).toString("base64url");
}

export function decodeCursor(cursor: string): string {
  try {
    const parsed: unknown = JSON.parse(Buffer.from(cursor, "base64url").toString("utf8"));
    if (parsed !== null && typeof parsed === "object") {
      const { v, s } = parsed as { v?: unknown; s?: unknown };
      if (v === 1 && typeof s === "string" && /^\d{1,19}$/.test(s) && BigInt(s) <= MAX_BIGINT) return s;
    }
  } catch {
    // cai no erro abaixo
  }
  throw new InvalidQueryError("cursor inválido");
}
