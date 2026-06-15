FROM node:22-alpine

WORKDIR /app

RUN corepack enable && corepack prepare pnpm@latest --activate

COPY package.json pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile

COPY tsconfig.json eslint.config.mjs ./
COPY src/ ./src/

RUN pnpm build

ENV NODE_ENV=production

CMD ["node", "dist/sync-mongo-postgre.js"]
