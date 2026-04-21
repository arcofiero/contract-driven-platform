"""Launch all 4 Kafka producers in parallel daemon threads."""
import logging
import threading
import time

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(threadName)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def _run_producer(name: str, target_fn):
    logger.info("Starting producer thread: %s", name)
    try:
        target_fn()
    except Exception as exc:
        logger.exception("Producer %s crashed: %s", name, exc)


def main():
    from producers.orders_producer import run as run_orders
    from producers.clickstream_producer import run as run_clickstream
    from producers.payments_producer import run as run_payments
    from producers.weather_producer import run as run_weather

    producers = [
        ("orders", run_orders),
        ("clickstream", run_clickstream),
        ("payments", run_payments),
        ("weather", run_weather),
    ]

    threads = []
    for name, fn in producers:
        t = threading.Thread(
            target=_run_producer,
            args=(name, fn),
            name=f"producer-{name}",
            daemon=True,
        )
        t.start()
        threads.append(t)
        logger.info("Thread started: producer-%s", name)

    logger.info("All %d producers running. Press Ctrl+C to stop.", len(threads))
    try:
        while True:
            time.sleep(5)
            alive = [t.name for t in threads if t.is_alive()]
            dead = [t.name for t in threads if not t.is_alive()]
            if dead:
                logger.warning("Dead producer threads: %s", dead)
            logger.debug("Alive threads: %s", alive)
    except KeyboardInterrupt:
        logger.info("Shutdown signal received — waiting for producers to flush...")
        # Daemon threads exit automatically; give them a moment to flush
        time.sleep(5)
        logger.info("Shutdown complete.")


if __name__ == "__main__":
    main()
