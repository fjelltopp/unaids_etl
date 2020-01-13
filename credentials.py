import logging
import os

from dotenv import load_dotenv

logger = logging.getLogger(__name__)
logger.setLevel("INFO")


def read_credentials(dhis2_credentials_file):
    if dhis2_credentials_file and os.path.exists(dhis2_credentials_file):
        logger.info(f"Loading DHIS2 credentials from file {dhis2_credentials_file}")
        load_dotenv(dhis2_credentials_file)
        return True
    return False
