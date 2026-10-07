import { createOrm } from "../persistence/orm";
import { MikroOrmUnitOfWork } from "../persistence/unit-of-work";

const DEFAULT_URL = "postgres://wager:wager@localhost:5433/wager";
const USAGE = "uso: bun run outbox:requeue <--list | --all | eventId...>";

/**
 * Operação manual da quarentena da outbox (ARCHITECTURE.md §12.3):
 *   --list      lista os eventos em quarentena, com o último erro;
 *   --all       devolve todos à fila;
 *   eventId...  devolve só esses.
 * O evento devolvido volta com attempts = 0 e é publicado na próxima rodada do relay; se eventos posteriores da mesma
 * wallet já saíram, ele sai depois deles (o consumidor ordena pelo `walletVersion` / deduplica pelo `eventId`).
 */
const args = process.argv.slice(2);
if (args.length === 0) {
  console.error(USAGE);
  process.exit(2);
}

const orm = await createOrm(process.env["DATABASE_URL"] ?? DEFAULT_URL, 1);
try {
  const uow = new MikroOrmUnitOfWork(orm);
  if (args[0] === "--list") {
    const rows = await orm.em.getConnection().execute(
      `SELECT id, aggregate_id, event_type, attempts, quarantined_at, last_error
         FROM outbox_messages WHERE quarantined_at IS NOT NULL ORDER BY seq`,
    );
    console.log(JSON.stringify(rows, null, 2));
  } else {
    const ids = args[0] === "--all" ? undefined : args;
    if (ids?.some((id) => !/^[0-9a-f-]{36}$/i.test(id))) {
      console.error(`ids inválidos. ${USAGE}`);
      process.exit(2);
    }
    const requeued = await uow.run((repos) => repos.outbox.requeueQuarantined(ids));
    console.log(JSON.stringify({ requeued }));
  }
} finally {
  await orm.close();
}
