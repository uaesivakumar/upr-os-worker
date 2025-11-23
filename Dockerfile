# UPR OS Worker - Unified Async Pipeline Processor
FROM node:20-alpine

WORKDIR /app

# Create non-root user first
RUN addgroup -g 1001 -S nodejs && adduser -S worker -u 1001

# Install dependencies
COPY package.json ./
RUN npm install --only=production

# Copy source and set ownership
COPY --chown=worker:nodejs src ./src

# Switch to non-root user
USER worker

# Environment
ENV NODE_ENV=production
ENV PORT=8080

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD node -e "require('http').get('http://localhost:8080/health', (r) => {process.exit(r.statusCode === 200 ? 0 : 1)})"

CMD ["node", "src/server.js"]
