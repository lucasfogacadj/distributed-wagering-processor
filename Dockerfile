FROM oven/bun:1 AS deps
WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile --production

FROM oven/bun:1
WORKDIR /app
ENV NODE_ENV=production
COPY --from=deps /app/node_modules ./node_modules
COPY package.json tsconfig.json ./
COPY src ./src
COPY migrations ./migrations

USER bun
EXPOSE 3000

# Liveness: só confirma que o processo responde. Readiness (PostgreSQL + SQS) fica em /health/ready.
HEALTHCHECK --interval=5s --timeout=3s --start-period=20s --retries=10 \
  CMD bun -e "fetch('http://127.0.0.1:' + (process.env.PORT ?? 3000) + '/health/live').then(r => process.exit(r.ok ? 0 : 1)).catch(() => process.exit(1))"

# bun como PID 1 recebe o SIGTERM direto: o shutdown gracioso (drenar o consumidor, devolver a visibilidade
# das mensagens, fechar o ORM) é acionado pelo `docker stop`.
CMD ["bun", "run", "src/main.ts"]
