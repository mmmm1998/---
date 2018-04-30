import logging

logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')
ch = logging.StreamHandler()
ch.setLevel('DEBUG')
ch.setFormatter(logging.Formatter("[%(filename)s:%(funcName)s:%(lineno)s]%(levelname)s: %(message)s"))
logger.addHandler(ch) 
