from loguru import logger

def info(message: str):
    logger.info(message)

def debug( message: str):
    logger.debug(message)

def warn( message: str):
    logger.warning (message)

def err( message: str):
    logger.error(message)



