import { Body, Controller, Get, Headers, HttpCode, Inject, Param, Post, Query, Res } from "@nestjs/common";
import type { Response } from "express";
import type { CreateWallet } from "../../application/create-wallet";
import type { ProviderIdentity } from "../../application/ports";
import type { WalletQueries } from "../../application/queries";
import { assertMayReadWallet, assertOperator, CurrentIdentity } from "./auth.guard";
import { createWalletSchema, parse, parseLedgerQuery, uuidSchema } from "./dto";
import { TOKENS } from "./tokens";

/**
 * Autorização: criar wallet, ler o ledger e reconciliar são operações de backoffice (operador); o saldo pode ser lido
 * por qualquer provedor autenticado, que precisa dele para operar. O ledger expõe as transações de TODOS os provedores
 * da wallet, por isso não é aberto a um provedor.
 */
@Controller("wallets")
export class WalletsController {
  constructor(
    @Inject(TOKENS.CREATE_WALLET) private readonly createWallet: CreateWallet,
    @Inject(TOKENS.QUERIES) private readonly queries: WalletQueries,
  ) {}

  @Post()
  async create(
    @Body() body: unknown,
    @Headers("x-correlation-id") correlationId: string,
    @CurrentIdentity() identity: ProviderIdentity,
    @Res() res: Response,
  ): Promise<void> {
    assertOperator(identity);
    const input = parse(createWalletSchema, body);
    const wallet = await this.createWallet.execute({ ...input, correlationId });
    res.status(201).json(wallet);
  }

  @Get(":walletId")
  get(@Param("walletId") walletId: string, @CurrentIdentity() identity: ProviderIdentity) {
    assertMayReadWallet(identity);
    return this.queries.getWallet(parse(uuidSchema, walletId));
  }

  @Get(":walletId/ledger")
  ledger(
    @Param("walletId") walletId: string,
    @Query("cursor") cursor: unknown,
    @Query("limit") limit: unknown,
    @CurrentIdentity() identity: ProviderIdentity,
  ) {
    assertOperator(identity);
    const id = parse(uuidSchema, walletId);
    return this.queries.listLedger(id, parseLedgerQuery({ cursor, limit }));
  }

  @Post(":walletId/reconciliation")
  @HttpCode(200)
  reconcile(@Param("walletId") walletId: string, @CurrentIdentity() identity: ProviderIdentity) {
    assertOperator(identity);
    return this.queries.reconcile(parse(uuidSchema, walletId));
  }
}
