import logging
import astroplant_client
from kit import Kit
import config

if __name__ == "__main__":
    # Logging
    ## create logger
    logger = logging.getLogger("AstroPlant")
    logger.setLevel(logging.DEBUG)

    ## create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    ## create formatter
    formatter = logging.Formatter('%(asctime)s - %(threadName)s - %(name)s - %(levelname)s - %(message)s')

    ## add formatter to ch
    ch.setFormatter(formatter)

    ## add ch to logger
    logger.addHandler(ch)

    logger.info('Reading configuration.')
    try:
        conf = config.read_config()
    except e:
        logger.error('Exception while reading configuration: %s' % e)
    
    logger.info('Creating AstroPlant network client.')
    api_client = astroplant_client.Client(conf["api"]["root"], conf["websockets"]["url"])

    logger.info('Authenticating AstroPlant network client.')
    api_client.authenticate(conf["auth"]["serial"], conf["auth"]["secret"])
    
    logger.info('Initialising kit.')
    kit = Kit(api_client, conf["debug"])
    kit.run()
