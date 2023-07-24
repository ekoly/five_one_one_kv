import logging
import logging.handlers
import sys
from concurrent.futures import ThreadPoolExecutor

from five_one_one_kv.c import server

logger = logging.getLogger()
logger.handlers.clear()
formatter = logging.Formatter("%(asctime)s | %(name)s | %(levelname)s | %(message)s")
sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(formatter)
logger.addHandler(sh)
wfh = logging.handlers.WatchedFileHandler("server.log")
wfh.setFormatter(formatter)
logger.addHandler(wfh)
logger.setLevel(logging.DEBUG)


class Server:
    def __init__(self, port=8513, num_threads=4):
        if num_threads < 2 or num_threads > 16:
            raise ValueError("num_threads must be in [2, 16]")
        try:
            self._server = server(port=port, num_threads=num_threads)
        except Exception:
            logger.exception("server failed to initialize")
            raise
        logger.info("server has initialized")
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            executor.submit(self._server.poll_loop)
            for _ in range(num_threads - 1):
                executor.submit(self._server.io_loop)


if __name__ == "__main__":
    Server()
