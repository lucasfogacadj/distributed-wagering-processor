import { Catch, HttpException, Inject, type ArgumentsHost, type ExceptionFilter } from "@nestjs/common";
import type { Request, Response } from "express";
import { classifyError } from "../../application/error-classification";
import { IdempotencyConflictError, ResourceNotFoundError, WalletAlreadyExistsError } from "../../application/errors";
import type { LoggerPort } from "../../application/ports";
import { DomainError } from "../../domain/errors";
import { RequestValidationError } from "./dto";
import { TOKENS } from "./tokens";

interface ApiError {
  status: number;
  code: string;
  message: string;
  details?: unknown;
  retryAfterSeconds?: number;
}

/**
 * Mapa único de erro → HTTP, igual em todos os endpoints:
 *   400 payload/consulta inválidos     · 404 recurso inexistente
 *   409 conflito (idempotência, wallet duplicada)
 *   422 rejeição de negócio            · 503 falha transitória (reenviar com o mesmo Idempotency-Key)
 *   500 falha não prevista (sem detalhes internos na resposta)
 * (202 PENDING_REFERENCE e 422 REJECTED de transações são resultados, não exceções: ver WagerController.)
 */
@Catch()
export class ApiExceptionFilter implements ExceptionFilter {
  constructor(@Inject(TOKENS.LOGGER) private readonly logger: LoggerPort) {}

  catch(exception: unknown, host: ArgumentsHost): void {
    const http = host.switchToHttp();
    const response = http.getResponse<Response>();
    const request = http.getRequest<Request>();

    const error = this.translate(exception);
    const context = {
      method: request.method,
      path: request.path,
      status: error.status,
      code: error.code,
      correlationId: request.headers["x-correlation-id"],
    };

    // 503 é falha transitória esperada (lock timeout, deadlock, banco reiniciando): warn, sem stack.
    if (error.status === 503) {
      this.logger.warn("request_transient_failure", context);
    } else if (error.status >= 500) {
      this.logger.error("request_failed", {
        ...context,
        error: exception instanceof Error ? { name: exception.name, message: exception.message, stack: exception.stack } : String(exception),
      });
    }

    if (error.retryAfterSeconds !== undefined) response.setHeader("Retry-After", String(error.retryAfterSeconds));
    response.status(error.status).json({
      error: { code: error.code, message: error.message, ...(error.details !== undefined ? { details: error.details } : {}) },
    });
  }

  private translate(exception: unknown): ApiError {
    if (exception instanceof RequestValidationError) {
      return { status: 400, code: exception.code, message: exception.message, details: exception.issues };
    }

    if (exception instanceof HttpException) {
      const status = exception.getStatus();
      return {
        status,
        code: httpCode(status),
        message: exception.message,
        ...(status === 503 ? { retryAfterSeconds: 1 } : {}),
      };
    }

    // Erros do body-parser do Express (JSON malformado, payload grande demais) chegam com `status`/`type`.
    const parserStatus = (exception as { status?: unknown; type?: unknown } | null)?.status;
    if (typeof parserStatus === "number" && parserStatus >= 400 && parserStatus < 500) {
      const type = (exception as { type?: unknown }).type;
      return {
        status: parserStatus,
        code: parserStatus === 400 ? "INVALID_REQUEST" : httpCode(parserStatus),
        message: type === "entity.parse.failed" ? "JSON malformado" : "Requisição inválida",
      };
    }

    switch (classifyError(exception)) {
      case "transient":
        return {
          status: 503,
          code: "SERVICE_UNAVAILABLE",
          message: "Falha transitória de infraestrutura; reenvie a requisição",
          retryAfterSeconds: 1,
        };
      case "invalid": {
        const e = exception as DomainError;
        return { status: 400, code: e.code, message: e.message };
      }
      case "business": {
        const e = exception as DomainError;
        if (e instanceof ResourceNotFoundError) return { status: 404, code: e.code, message: e.message };
        if (e instanceof IdempotencyConflictError || e instanceof WalletAlreadyExistsError) {
          return { status: 409, code: e.code, message: e.message };
        }
        return { status: 422, code: e.code, message: e.message };
      }
      default:
        return { status: 500, code: "INTERNAL_ERROR", message: "Erro interno" };
    }
  }
}

function httpCode(status: number): string {
  switch (status) {
    case 400:
      return "BAD_REQUEST";
    case 401:
      return "UNAUTHORIZED";
    case 403:
      return "FORBIDDEN";
    case 404:
      return "NOT_FOUND";
    case 503:
      return "SERVICE_UNAVAILABLE";
    case 405:
      return "METHOD_NOT_ALLOWED";
    case 413:
      return "PAYLOAD_TOO_LARGE";
    default:
      return `HTTP_${status}`;
  }
}
