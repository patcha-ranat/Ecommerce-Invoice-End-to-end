import logging


def set_logger(level_name: str):
    # set logger
    logger = logging.getLogger(level_name)
    logger.setLevel(logging.DEBUG)
    
    # set formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt="%Y-%m-%d %I:%M:%S %p")
    
    # set handler
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    logger.addHandler(sh)

    return logger