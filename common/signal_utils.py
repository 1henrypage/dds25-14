import signal
import sys
import logging
import asyncio

async def handle_sigterm(signum, frame, delay: int = 3):
    """Gracefully handle SIGTERM and delay shutdown."""
    logging.info("RECEIVED SIGTERM")

    # Set the flag to False to stop the service from handling requests
    global allow_flag
    allow_flag = False

    # Sleep for the specified delay
    await asyncio.sleep(delay)

    logging.info("Exiting after %d seconds.", delay)
    sys.exit(0)

def register_sigterm_handler(delay: int = 3):
    signal.signal(signal.SIGTERM, lambda signum, frame: asyncio.create_task(handle_sigterm(signum, frame, delay)))
