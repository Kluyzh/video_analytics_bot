from logging import DEBUG, Formatter, StreamHandler, getLogger
from sys import stdout

logger = getLogger(__name__)
formatter = Formatter('%(asctime)s [%(levelname)s] %(message)s')
handler = StreamHandler(stream=stdout)
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(DEBUG)
