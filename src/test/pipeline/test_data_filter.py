import unittest
from shared import schema, test_utils
from pipeline.data_filter import filter_repeated_process_step, fix_device_full_short_name


class DataFilter(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.context = test_utils.TestContext.getInstance()
        cls.schemas = schema.Schema()

    def test_filter_repeated_process_step(self):
        batches = test_utils.read_csv(self.context, 'input_filter_repeated_process.csv',
                                      self.schemas.input_data_filter_batches)
        expected = test_utils.read_csv(self.context,
                                       'expected_filter_repeated_process.csv',
                                       self.schemas.expected_data_filter_batches)
        actual = filter_repeated_process_step(batches)
        self.assertTrue(test_utils.equals(actual.select(expected.columns), expected))


if __name__ == '__main__':
    unittest.main()
