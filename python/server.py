import argparse
import logging
from concurrent.futures import ThreadPoolExecutor

from five_one_one_kv.c import server

logger = logging.getLogger(__name__)


class Server:
    def __init__(self, port=8513, num_threads=4):
        if num_threads < 4 or num_threads > 16:
            raise ValueError("num_threads must be in [4, 16]")
        try:
            self._server = server(port=port, num_threads=num_threads)
        except Exception:
            logger.exception("server failed to initialize")
            raise
        logger.info("server has initialized")
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            executor.submit(self._server.poll_loop)
            executor.submit(self._server.storage_ttl_loop)
            for _ in range(num_threads - 2):
                executor.submit(self._server.io_loop)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="count", default=0)
    args = parser.parse_args()
    if args.verbose > 0:
        import logging
        import logging.handlers
        import sys

        logger = logging.getLogger()
        logger.handlers.clear()
        formatter = logging.Formatter(
            "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
        )
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(formatter)
        logger.addHandler(sh)
        wfh = logging.handlers.WatchedFileHandler("server.log")
        wfh.setFormatter(formatter)
        logger.addHandler(wfh)
        if args.verbose == 1:
            logger.setLevel(logging.INFO)
        else:
            logger.setLevel(logging.DEBUG)

    my_server = Server()
