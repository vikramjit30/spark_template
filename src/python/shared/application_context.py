""" TestContext._test_context = ApplicationContext(config)
Class initializes the spark context and all variables from the dpr config
"""

import logging
from pyspark.sql import SparkSession

LOGGER = logging.getLogger("covestro_etl." + __name__)


class ApplicationContext(object):
    """
    loads spark context and all the variables from project.conf
    """

    def __init__(self, config):
        """ creates an ApplicationContext using the given configs.
        """
        self.config = config
        self._set_config()
        self._create_spark_context()

    def _set_config(self) -> None:
        """
        Sets a new config (e.g. when a config for a different country is initialized)
        This makes sure, that the internal ApplicationContext settings are updated accordingly.

        :return: None
        """

        # Declare all the variables here via
        self.project_name = self.config.get('project', 'name')
        self.output_path = self.config.get('dev', 'output_path')

        LOGGER.info("Config in the application context was set")

    def _create_spark_context(self) -> None:
        """ helper creating the spark context.
        :return: None
        """
        spark_app_name = self.config.get('project', 'name')
        # create & configure spark session
        session_builder = SparkSession.builder \
            .appName(spark_app_name) \
            .master(self.config.get('spark', 'spark.master')) \
            .enableHiveSupport()

        self.spark = session_builder.config("spark.executor.memory",
                                            self.config.get('spark', 'spark.executor.memory')).config(
            "spark.driver.memory", self.config.get('spark', 'spark.driver.memory')).getOrCreate()
