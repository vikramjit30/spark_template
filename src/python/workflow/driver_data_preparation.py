"""Runs the module to download and store the data from Amazon s3
"""
import logging
import configparser
import pyspark.sql.functions as F

from shared.application_context import ApplicationContext
from pipeline.data_preparation import join_operations
from pipeline.data_filter import filter_repeated_process_step, fix_device_full_short_name
from pipeline.data_io import read_parquet_data, save_data_parquet

LOGGER = logging.getLogger("src." + __name__)


def main() -> None:
    """ Extract the variables from proj.conf and calls the run method.
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
    """
     Reads the downloaded S3 data and apply basic data preparation filters
    :param context: ApplicationContext
    :return: None
    """

    # read all parquet files and convert them into dataframes
    batches = read_parquet_data(context, context.s3_path_batch + '/' + 'batches.parquet')
    device = read_parquet_data(context, context.s3_path_batch + '/' + 'devices.parquet')
    sensors = read_parquet_data(context, context.s3_path_batch + '/' + 'sensors.parquet')
    all_sensors = read_parquet_data(context, context.s3_path_sensors)

    # remove redundant data from
    batches = filter_repeated_process_step(batches)
    # map the device full name to short name
    batches = fix_device_full_short_name(batches)

    # Join all the sensors
    all_sensor_with_devices, bd_with_sensors, batches_and_devices = join_operations(batches,
                                                                                    device, sensors, all_sensors)
    # Final query to setup
    df = all_sensor_with_devices.join(bd_with_sensors, on=['device_name_short', 'sensor_name',
                                                           'device_type', 'sensor_type']).filter(
        batches_and_devices.process_step_start < all_sensor_with_devices.datetime).filter(
        all_sensor_with_devices.datetime < batches_and_devices.process_step_end)

    df = df.withColumn("date_diff_min", (F.col("datetime").cast("long") -
                                         F.col("process_step_start").cast("long")) / 60.)
    query_ds = df.drop(*['batch_start_time', 'process_step_serial', '__index_level_0__',
                         'process_step_repetition_count'])

    # Save data parquet format
    save_data_parquet(query_ds, context.query_ds_path, partition_key="process_step_name")


if __name__ == '__main__':
    main()
