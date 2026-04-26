# ── Stage 1: Node.js deps ──
FROM node:20-slim AS node-deps

WORKDIR /build
COPY package.json package-lock.json ./
RUN npm ci --production

# ── Stage 2: Runtime ──
FROM mcr.microsoft.com/powershell:7.4-ubuntu-22.04

# Install Node.js 20
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates gnupg \
    && mkdir -p /etc/apt/keyrings \
    && curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key \
       | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg \
    && echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_20.x nodistro main" \
       > /etc/apt/sources.list.d/nodesource.list \
    && apt-get update && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

# Install ExchangeOnlineManagement PowerShell module
RUN pwsh -Command "Set-PSRepository PSGallery -InstallationPolicy Trusted; \
    Install-Module ExchangeOnlineManagement -Scope AllUsers -Force -AcceptLicense"

# Install tsx globally for running TypeScript
RUN npm install -g tsx

WORKDIR /app

# Copy Node.js deps
COPY --from=node-deps /build/node_modules ./node_modules
COPY package.json package-lock.json ./

# Copy worker scripts
COPY config.ps1 run.ps1 ./
COPY Part1-UniversalDomainSetup.ts ./
COPY Part2-MicrosoftRoomMailbox.ps1 ./
COPY Part2-MicrosoftInboxMutations.ps1 ./
COPY Part3-MicrosoftDomainCancellation.ps1 ./
COPY RecoveryCommon.ps1 ./
COPY Part4-MicrosoftDomainRecoveryMove.ps1 ./
COPY Part4-MicrosoftDomainRecoveryValidate.ps1 ./

# Copy the health-check wrapper
COPY healthcheck-wrapper.ps1 ./

# Healthcheck: wrapper touches heartbeat file each poll cycle
HEALTHCHECK --interval=60s --timeout=5s --start-period=60s --retries=3 \
    CMD test -f /tmp/pixie_microsoft_heartbeat && \
        find /tmp/pixie_microsoft_heartbeat -newermt '-120 seconds' | grep -q .

CMD ["pwsh", "./healthcheck-wrapper.ps1"]
