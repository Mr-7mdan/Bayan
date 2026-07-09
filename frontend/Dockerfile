# Bayan frontend — Next.js 15 standalone production server.
# Multi-stage: build with full toolchain, ship only the standalone output.
# REQUIRES `output: 'standalone'` in next.config.js (see DEPLOY_DOCKER.md — not
# yet set; without it .next/standalone is not produced and the COPY below fails).
FROM node:20-alpine AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
# NEXT_PUBLIC_ vars are inlined at build time. The value also feeds the
# next.config.js rewrite, so the standalone server proxies same-origin /api/*
# to the backend inside the compose network.
ARG NEXT_PUBLIC_API_BASE_URL=http://backend:8000/api
ENV NEXT_PUBLIC_API_BASE_URL=$NEXT_PUBLIC_API_BASE_URL
RUN npm run build

FROM node:20-alpine
WORKDIR /app
ENV NODE_ENV=production \
    HOSTNAME=0.0.0.0 \
    PORT=3000
# node:20-alpine already ships a non-root `node` user (uid 1000).
COPY --from=build --chown=node:node /app/.next/standalone ./
COPY --from=build --chown=node:node /app/.next/static ./.next/static
COPY --from=build --chown=node:node /app/public ./public
USER node
EXPOSE 3000
CMD ["node", "server.js"]
