FROM oven/bun:1

WORKDIR /app

COPY . .

ENV DATABASE_URL=postgresql://perps:perps@localhost:5432/perps

RUN bun install --frozen-lockfile
RUN cd packages/db && bunx prisma generate

# Set by CI (docker.yml): which release/commit this image is, reported by GET /health.
ARG VERSION=dev
ARG GIT_SHA=unknown
ENV VERSION=$VERSION GIT_SHA=$GIT_SHA

CMD ["bun", "apps/api/index.ts"]
