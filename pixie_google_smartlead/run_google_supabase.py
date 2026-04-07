import logging
import sys
import threading
import time

from app import setup_logging
from app.workers.google_inbox_lifecycle_worker import GoogleInboxLifecycleWorker
from app.workers.google_supabase_worker import GoogleSupabaseWorker

HEALTH_CHECK_INTERVAL = 30


if __name__ == "__main__":
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info("Starting Supabase Google workers...")
    provision_worker = GoogleSupabaseWorker()
    lifecycle_worker = GoogleInboxLifecycleWorker()

    threads = [
        threading.Thread(target=provision_worker.run_forever, name="google-provision-worker", daemon=True),
        threading.Thread(target=lifecycle_worker.run_forever, name="google-lifecycle-worker", daemon=True),
    ]
    for thread in threads:
        thread.start()

    while True:
        dead = [t for t in threads if not t.is_alive()]
        if dead:
            names = ", ".join(t.name for t in dead)
            logger.critical("Worker thread(s) died: %s — exiting for restart", names)
            sys.exit(1)
        time.sleep(HEALTH_CHECK_INTERVAL)
