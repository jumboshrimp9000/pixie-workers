import logging
import sys
import threading
import time

from app import setup_logging
from app.workers.nonprofit_google_cancel_worker import NonprofitGoogleCancelWorker
from app.workers.nonprofit_google_provision_worker import NonprofitGoogleProvisionWorker


HEARTBEAT_PATH = "/tmp/pixie_google_smartlead_heartbeat"
HEALTH_CHECK_INTERVAL = 30


def _touch_heartbeat() -> None:
    try:
        with open(HEARTBEAT_PATH, "w") as fh:
            fh.write(str(time.time()))
    except OSError:
        pass


def main() -> None:
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info("Starting nonprofit Google workers (provision + cancel)")

    provision_worker = NonprofitGoogleProvisionWorker()
    cancel_worker = NonprofitGoogleCancelWorker()

    threads = [
        threading.Thread(target=provision_worker.run_forever, name="nonprofit-google-provision-worker", daemon=True),
        threading.Thread(target=cancel_worker.run_forever, name="nonprofit-google-cancel-worker", daemon=True),
    ]
    for thread in threads:
        thread.start()

    while True:
        dead = [thread for thread in threads if not thread.is_alive()]
        if dead:
            logger.critical("Worker thread(s) died: %s", ", ".join(thread.name for thread in dead))
            sys.exit(1)
        _touch_heartbeat()
        time.sleep(HEALTH_CHECK_INTERVAL)


if __name__ == "__main__":
    main()
