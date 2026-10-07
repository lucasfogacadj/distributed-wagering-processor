import { SetMetadata } from "@nestjs/common";

export const IS_PUBLIC_KEY = "isPublic";

/** Rota aberta: o AuthGuard não consulta o IdP. Usado em health e métricas (o desafio exige health sem autenticação). */
export const Public = () => SetMetadata(IS_PUBLIC_KEY, true);
