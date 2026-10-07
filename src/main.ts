// PRIMEIRO import, de propósito: instala os gatilhos de shutdown antes de carregar o resto (ver src/shutdown.ts).
import { log, shutdown } from "./shutdown";
import "reflect-metadata";
import { NestFactory } from "@nestjs/core";
import { Pool } from "pg";
import { loadConfig } from "./config";
import { migrateUp } from "./infrastructure/database/migrator";
import { AppModule } from "./interfaces/http/app.module";

async function bootstrap(): Promise<void> {
  const config = loadConfig();

  if (config.runMigrations) {
    const pool = new Pool({ connectionString: config.databaseUrl, max: 1 });
    try {
      const applied = await migrateUp(pool);
      log("info", "migrations_applied", { migrations: applied });
    } finally {
      await pool.end();
    }
  }

  // O logger do Nest fica desligado: todo log da aplicação sai estruturado (JSON) pelo LoggerPort.
  const app = await NestFactory.create(
    AppModule.register({
      databaseUrl: config.databaseUrl,
      poolMax: config.poolMax,
      lockTimeoutMs: config.lockTimeoutMs,
      auth: config.auth,
      ...(config.sqs.enabled
        ? { messaging: { sqs: config.sqs, workers: config.workers, instanceId: config.instanceId } }
        : {}),
    }),
    { logger: false },
  );
  shutdown.created(app);
  // Shutdown pedido durante o create: não sobe o HTTP nem os workers; `booted` fecha a aplicação na sequência.
  if (!shutdown.requested) {
    await app.listen(config.port, "0.0.0.0");
    log("info", "listening", { port: config.port, auth: config.auth.mode });
  }
  await shutdown.booted();
}

bootstrap().catch((error: unknown) => {
  log("error", "bootstrap_failed", { error: String(error) });
  process.exit(1);
});
