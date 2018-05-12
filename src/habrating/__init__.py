import logging

logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')

# Log file
fileh = logging.FileHandler('habrating.log')
fileh.setLevel('DEBUG')
fileh.setFormatter(logging.Formatter("[%(filename)s:%(funcName)s:%(lineno)s]%(levelname)s: %(message)s"))

ch = logging.StreamHandler()
ch.setLevel('WARN')
ch.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))

logger.addHandler(fileh) 
logger.addHandler(ch) 
