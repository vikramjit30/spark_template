import os
import configparser
from shared.application_context import ApplicationContext

test_base_dir = "file:///" + os.path.join(os.path.dirname(__file__), '../../test/resources/')


def read_csv(context, file, schema):
    df = context.spark.read.format('com.databricks.spark.csv') \
        .options(header='true') \
        .load(test_base_dir + file, schema=schema)

    return df


def equals(df1, df2):
    return len(df1.subtract(df2).take(1)) == 0 and len(df2.subtract(df1).take(1)) == 0


class TestContext:
    """ Helper class providing an ApplicationContext Singleton to test classes """
    _test_context = None

    @staticmethod
    def getInstance() -> ApplicationContext:
        """ returns a singleton instance of an Application context for the test classes

        :return: ApplicationContext for the test classes
        """
        if TestContext._test_context is None:

            # get config
            config = configparser.ConfigParser()
            # parse existing file
            config.read('../../config/project.conf')
            TestContext._test_context = ApplicationContext(config)

        return TestContext._test_context
