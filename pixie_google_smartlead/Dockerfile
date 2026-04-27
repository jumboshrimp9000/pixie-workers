# ── Stage 1: Install Python deps ──
FROM python:3.11-slim AS deps

WORKDIR /build
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ── Stage 2: Runtime ──
FROM python:3.11-slim

# Playwright system dependencies + Chromium
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    debsig-verify \
    gnupg \
    libnss3 libnspr4 libdbus-1-3 libatk1.0-0 libatk-bridge2.0-0 \
    libcups2 libdrm2 libxkbcommon0 libatspi2.0-0 libxcomposite1 \
    libxdamage1 libxfixes3 libxrandr2 libgbm1 libpango-1.0-0 \
    libcairo2 libasound2 libwayland-client0 \
    && rm -rf /var/lib/apt/lists/*

# 1Password CLI is required for service-account backed TOTP enrollment.
RUN set -eux; \
    arch="$(dpkg --print-architecture)"; \
    curl -sS https://downloads.1password.com/linux/keys/1password.asc \
        | gpg --dearmor --output /usr/share/keyrings/1password-archive-keyring.gpg; \
    echo "deb [arch=${arch} signed-by=/usr/share/keyrings/1password-archive-keyring.gpg] https://downloads.1password.com/linux/debian/${arch} stable main" \
        > /etc/apt/sources.list.d/1password.list; \
    mkdir -p /etc/debsig/policies/AC2D62742012EA22 /usr/share/debsig/keyrings/AC2D62742012EA22; \
    curl -sS https://downloads.1password.com/linux/debian/debsig/1password.pol \
        > /etc/debsig/policies/AC2D62742012EA22/1password.pol; \
    curl -sS https://downloads.1password.com/linux/keys/1password.asc \
        | gpg --dearmor --output /usr/share/debsig/keyrings/AC2D62742012EA22/debsig.gpg; \
    apt-get update; \
    apt-get install -y --no-install-recommends 1password-cli; \
    op --version; \
    rm -rf /var/lib/apt/lists/*

# Copy installed Python packages
COPY --from=deps /install /usr/local

# Install Playwright Chromium browser
RUN playwright install chromium

WORKDIR /app

# Copy worker code
COPY app/ ./app/
COPY main.py .
COPY main_nonprofit.py .
COPY run_google_supabase.py .
COPY scripts/ ./scripts/

# Healthcheck: worker touches this file every poll cycle
HEALTHCHECK --interval=60s --timeout=5s --start-period=30s --retries=3 \
    CMD test -f /tmp/pixie_google_smartlead_heartbeat && \
        find /tmp/pixie_google_smartlead_heartbeat -newermt '-120 seconds' | grep -q .

CMD ["python3", "main.py"]
