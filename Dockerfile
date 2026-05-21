FROM node:22-alpine

WORKDIR /app

COPY package*.json ./
RUN npm install --production=false

COPY tsconfig.json ./
COPY src/ ./src/

RUN npm run build

ENV NODE_ENV=production

CMD ["node", "dist/sync-mongo-postgre.js"]
