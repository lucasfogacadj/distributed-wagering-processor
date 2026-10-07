import { describe, expect, it } from "bun:test";
import { decodeCursor, encodeCursor } from "../../../src/application/cursor";
import { InvalidQueryError } from "../../../src/application/errors";

describe("cursor", () => {
  it("faz o roundtrip do seq", () => {
    expect(decodeCursor(encodeCursor("42"))).toBe("42");
    expect(decodeCursor(encodeCursor("9223372036854775807"))).toBe("9223372036854775807");
  });

  it("é opaco: base64url, sem expor o seq", () => {
    const cursor = encodeCursor("123456");
    expect(cursor).toMatch(/^[A-Za-z0-9_-]+$/);
    expect(cursor).not.toContain("123456");
  });

  it("é determinístico", () => {
    expect(encodeCursor("7")).toBe(encodeCursor("7"));
  });

  it.each([
    ["vazio", ""],
    ["lixo", "não-é-cursor"],
    ["base64 de texto qualquer", Buffer.from("olá").toString("base64url")],
    ["versão desconhecida", Buffer.from(JSON.stringify({ v: 2, s: "1" })).toString("base64url")],
    ["seq negativo", Buffer.from(JSON.stringify({ v: 1, s: "-1" })).toString("base64url")],
    ["seq não numérico", Buffer.from(JSON.stringify({ v: 1, s: "abc" })).toString("base64url")],
    ["seq numérico não string", Buffer.from(JSON.stringify({ v: 1, s: 5 })).toString("base64url")],
    ["seq grande demais", Buffer.from(JSON.stringify({ v: 1, s: "1".repeat(30) })).toString("base64url")],
  ])("rejeita cursor inválido: %s", (_name, cursor) => {
    expect(() => decodeCursor(cursor)).toThrow(InvalidQueryError);
  });
});
