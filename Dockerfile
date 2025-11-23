# UPR OS Worker - Unified Async Pipeline Processor
FROM node:20-alpine

WORKDIR /app

# Install dependencies
COPY package.json ./
RUN npm install --only=production

# Copy source
COPY src ./src

# Create non-root user
RUN addgroup -g 1001 -S nodejs && adduser -S worker -u 1001
USER worker

# Environment
ENV NODE_ENV=production
ENV PORT=8080

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD node -e "require('http').get('http://localhost:8080/health', (r) => {process.exit(r.statusCode === 200 ? 0 : 1)})"

CMD ["node", "src/server.js"]
