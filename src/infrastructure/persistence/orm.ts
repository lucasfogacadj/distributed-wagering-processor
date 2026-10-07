import { MikroORM } from "@mikro-orm/postgresql";
import { ALL_SCHEMAS } from "./schemas";

export type AppOrm = MikroORM;

/** O MikroORM v7 entrega ao hook a conexão do Kysely, que executa uma consulta já compilada. */
interface KyselyConnection {
  executeQuery(query: { sql: string; parameters: readonly unknown[] }): Promise<unknown>;
}

/**
 * A estratégia transacional é `EntityManager.transactional()` em um fork por operação (Unit of Work
 * isolado): cada caso de uso roda em uma transação READ COMMITTED, e a serialização por wallet vem de
 * `SELECT … FOR UPDATE` (LockMode.PESSIMISTIC_WRITE) na linha da wallet.
 *
 * `lockTimeoutMs` define o `lock_timeout` de cada conexão do pool: quem espera demais por uma wallet falha
 * com 55P03 (transitório → HTTP 503 / retry na fila) em vez de ficar preso atrás de uma transação travada.
 */
export function createOrm(clientUrl: string, poolMax = 20, lockTimeoutMs?: number): Promise<AppOrm> {
  return MikroORM.init({
    clientUrl,
    entities: ALL_SCHEMAS,
    allowGlobalContext: false,
    pool: { min: 1, max: poolMax },
    discovery: { warnWhenNoEntities: false },
    ...(lockTimeoutMs !== undefined
      ? {
          // O MikroORM ignora `?options=-c lock_timeout=…` na URL; o hook roda em toda conexão nova do pool.
          onCreateConnection: async (connection: unknown) => {
            await (connection as KyselyConnection).executeQuery({
              sql: `SET lock_timeout = ${Math.trunc(lockTimeoutMs)}`,
              parameters: [],
            });
          },
        }
      : {}),
  });
}
