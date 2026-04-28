import logging
import os
import sys
import threading
import time

from app import setup_logging
from app.workers.google_cancel_worker import GoogleCancelWorker
from app.workers.google_inbox_lifecycle_worker import GoogleInboxLifecycleWorker
from app.workers.google_supabase_worker import GoogleSupabaseWorker

HEARTBEAT_PATH = "/tmp/pixie_google_smartlead_heartbeat"
HEALTH_CHECK_INTERVAL = 30  # seconds


def _touch_heartbeat() -> None:
    """Update heartbeat file so Docker HEALTHCHECK can verify liveness."""
    try:
        with open(HEARTBEAT_PATH, "w") as f:
            f.write(str(time.time()))
    except OSError:
        pass


def main() -> None:
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info("Starting pixie_google_smartlead workers (provisioning + lifecycle + cancellation)...")

    provision_worker = GoogleSupabaseWorker()
    lifecycle_worker = GoogleInboxLifecycleWorker()
    cancel_worker = GoogleCancelWorker()

    threads = [
        threading.Thread(target=provision_worker.run_forever, name="google-provision-worker", daemon=True),
        threading.Thread(target=lifecycle_worker.run_forever, name="google-lifecycle-worker", daemon=True),
        threading.Thread(target=cancel_worker.run_forever, name="google-cancel-worker", daemon=True),
    ]
    for thread in threads:
        thread.start()

    # Health-check loop: if any daemon thread dies, exit so Docker can restart
    while True:
        dead = [t for t in threads if not t.is_alive()]
        if dead:
            names = ", ".join(t.name for t in dead)
            logger.critical("Worker thread(s) died: %s — exiting for restart", names)
            sys.exit(1)

        _touch_heartbeat()
        time.sleep(HEALTH_CHECK_INTERVAL)


if __name__ == "__main__":
    main()
