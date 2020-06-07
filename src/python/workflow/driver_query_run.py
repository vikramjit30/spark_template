"""Runs the module to download and store the data from Amazon s3
"""
import logging
import configparser
from shared.application_context import ApplicationContext
from pipeline.data_preparation import query_param
from pipeline.data_io import read_parquet_data, save_as_csv

LOGGER = logging.getLogger("src." + __name__)

device_types = {'boiler', 'reactor/kv', 'reactor/p', 'reactor/k', 'reactor/d', 'tank'}
sensor_types = {'flow', 'temperature', 'pressure', 'revolutions', 'level'}
process_step_names = {'wait', 'stirring', 'distillation', 'finishing'}


def main(freq: int, sensor_type: str, device_type: str, process_step_name: str) -> None:
    """ Extract the variable from proj.conf and pass to run method.
    :return None
    """
    # parse existing file
    config = configparser.ConfigParser()
    config.read('../../config/project.conf')

    # create application context
    context = ApplicationContext(config)
    if _check_list(sensor_type, sensor_types) and _check_list(device_type, device_types) and \
            _check_list(process_step_name, process_step_names):
        run(context, freq=freq, sensor_type=sensor_type, device_type=device_type, process_step_name=process_step_name)


def _check_list(name, list_names: set) -> None:
    if name in list_names:
        return True
    else:
        raise ValueError(
            'Parameter {} passed is not part of parameter {}'.format(name, list_names))


def run(context: ApplicationContext, freq, sensor_type, device_type, process_step_name) -> None:
    """
     Reads the query data structure and process it for the given parameters
    :param context:
    :param freq: frequency of time interval
    :param sensor_type: type of the sensor, ex: temperature
    :param device_type: type of the device, ex: boiler
    :param process_step_name: process name, ex: distillation
    :return: None
    """

    query_ds = read_parquet_data(context, context.query_ds_path)
    final_df = query_param(df=query_ds, freq=freq, sensor_type=sensor_type,
                           device_type=device_type, process_step_name=process_step_name)
    # Save the data as CSV
    device_type = device_type.replace('/', '_')
    file_name = [process_step_name, device_type, sensor_type, str(freq)]
    file_name = "_".join(file_name)
    output_file_name = context.output_path + "/" + file_name
    save_as_csv(final_df, output_file_name)


if __name__ == '__main__':
    main(freq=10, sensor_type='flow', process_step_name='distillation', device_type='reactor/k')
