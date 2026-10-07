import type { Repositories, UnitOfWork } from "../../application/ports";
import type { AppOrm } from "./orm";
import { createRepositories } from "./repositories";

/** Cada `run` abre um fork do EntityManager (Unit of Work + Identity Map próprios) e uma transação SQL. */
export class MikroOrmUnitOfWork implements UnitOfWork {
  constructor(private readonly orm: AppOrm) {}

  run<T>(work: (repos: Repositories) => Promise<T>): Promise<T> {
    return this.orm.em.fork().transactional((em) => work(createRepositories(em)));
  }
}
