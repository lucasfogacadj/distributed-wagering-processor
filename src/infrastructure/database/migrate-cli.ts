import { Pool } from "pg";
import { migrateDown, migrateUp } from "./migrator";

const DEFAULT_URL = "postgres://wager:wager@localhost:5433/wager";

const command = process.argv[2];
if (command !== "up" && command !== "down") {
  console.error("uso: bun run src/infrastructure/database/migrate-cli.ts <up|down> [steps]");
  process.exit(2);
}

const pool = new Pool({ connectionString: process.env["DATABASE_URL"] ?? DEFAULT_URL, max: 1 });
try {
  const applied = command === "up" ? await migrateUp(pool) : await migrateDown(pool, Number(process.argv[3] ?? 1));
  console.log(JSON.stringify({ command, migrations: applied }));
} finally {
  await pool.end();
}
