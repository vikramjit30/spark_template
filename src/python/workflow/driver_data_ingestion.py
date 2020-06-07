"""Runs the module to download and store the data from Amazon s3
"""

from pipeline.data_io import load_and_parse_xml, save_s3_data
from shared.application_context import ApplicationContext
import logging
import configparser

logging.basicConfig(level=logging.NOTSET)
LOGGER = logging.getLogger("src." + __name__)


def main() -> None:
    """ Extract the variable from proj.conf and pass to run method.
    :return None
    """
    # get config
    config = configparser.ConfigParser()
    # parse existing file
    config.read('../../config/project.conf')

    # create application context
    context = ApplicationContext(config)
    run(context)


def run(context: ApplicationContext) -> None:
    """ Runs the driver using the context from ApplicationContext
    :param context: the config data
    :return: None
    """
    soup = load_and_parse_xml(context.url)
    save_s3_data(context, soup)


if __name__ == '__main__':
    main()
