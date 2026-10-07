import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import type { Pool, PoolClient } from "pg";

const MIGRATIONS_DIR = fileURLToPath(new URL("../../../migrations/", import.meta.url));
/** Chave do advisory lock que serializa migrations entre instâncias subindo ao mesmo tempo. */
const MIGRATION_LOCK_KEY = 7_391_204_001;

interface Migration {
  version: string;
  up: string;
  down: string;
}

function loadMigrations(dir = MIGRATIONS_DIR): Migration[] {
  const versions = readdirSync(dir)
    .map((f) => /^(\d{4}_[a-z0-9_]+)\.up\.sql$/.exec(f)?.[1])
    .filter((v): v is string => v !== undefined)
    .sort();

  return versions.map((version) => ({
    version,
    up: readFileSync(join(dir, `${version}.up.sql`), "utf8"),
    down: readFileSync(join(dir, `${version}.down.sql`), "utf8"),
  }));
}

async function withMigrationLock<T>(pool: Pool, fn: (client: PoolClient) => Promise<T>): Promise<T> {
  const client = await pool.connect();
  try {
    await client.query("SELECT pg_advisory_lock($1)", [MIGRATION_LOCK_KEY]);
    await client.query(
      `CREATE TABLE IF NOT EXISTS schema_migrations (
         version    text        PRIMARY KEY,
         applied_at timestamptz NOT NULL DEFAULT now()
       )`,
    );
    return await fn(client);
  } finally {
    await client.query("SELECT pg_advisory_unlock($1)", [MIGRATION_LOCK_KEY]).catch(() => undefined);
    client.release();
  }
}

async function runInTransaction(client: PoolClient, sql: string, record: () => Promise<unknown>): Promise<void> {
  await client.query("BEGIN");
  try {
    await client.query(sql);
    await record();
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  }
}

/** Aplica as migrations pendentes, cada uma em sua transação. Seguro para chamadas concorrentes. */
export async function migrateUp(pool: Pool): Promise<string[]> {
  return withMigrationLock(pool, async (client) => {
    const { rows } = await client.query<{ version: string }>("SELECT version FROM schema_migrations");
    const applied = new Set(rows.map((r) => r.version));
    const done: string[] = [];

    for (const m of loadMigrations().filter((m) => !applied.has(m.version))) {
      await runInTransaction(client, m.up, () =>
        client.query("INSERT INTO schema_migrations (version) VALUES ($1)", [m.version]),
      );
      done.push(m.version);
    }
    return done;
  });
}

/** Reverte as últimas `steps` migrations aplicadas (padrão: 1), da mais nova para a mais antiga. */
export async function migrateDown(pool: Pool, steps = 1): Promise<string[]> {
  return withMigrationLock(pool, async (client) => {
    const { rows } = await client.query<{ version: string }>(
      "SELECT version FROM schema_migrations ORDER BY version DESC LIMIT $1",
      [steps],
    );
    const byVersion = new Map(loadMigrations().map((m) => [m.version, m]));
    const done: string[] = [];

    for (const { version } of rows) {
      const migration = byVersion.get(version);
      if (!migration) throw new Error(`Migration aplicada sem arquivo correspondente: ${version}`);
      await runInTransaction(client, migration.down, () =>
        client.query("DELETE FROM schema_migrations WHERE version = $1", [version]),
      );
      done.push(version);
    }
    return done;
  });
}
