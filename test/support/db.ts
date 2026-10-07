import { randomUUID } from "node:crypto";
import { Pool } from "pg";
import { migrateUp } from "../../src/infrastructure/database/migrator";

const ADMIN_URL = process.env["TEST_ADMIN_DATABASE_URL"] ?? "postgres://wager:wager@localhost:5433/postgres";

export interface TestDatabase {
  name: string;
  url: string;
  pool: Pool;
  drop(): Promise<void>;
}

/** Cria um banco novo e isolado por suíte, para os testes não dependerem uns dos outros. */
export async function createTestDatabase(options: { migrate?: boolean } = {}): Promise<TestDatabase> {
  const name = `wager_test_${randomUUID().replaceAll("-", "")}`;
  const admin = new Pool({ connectionString: ADMIN_URL, max: 1 });
  await admin.query(`CREATE DATABASE ${name}`);
  await admin.end();

  const url = ADMIN_URL.replace(/\/[^/]*$/, `/${name}`);
  const pool = new Pool({ connectionString: url, max: 10 });
  if (options.migrate !== false) await migrateUp(pool);

  return {
    name,
    url,
    pool,
    async drop() {
      await pool.end();
      const cleanup = new Pool({ connectionString: ADMIN_URL, max: 1 });
      await cleanup.query(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`);
      await cleanup.end();
    },
  };
}
