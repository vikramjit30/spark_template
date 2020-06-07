"""
Methods related to data read/write/modify operations
"""
import urllib
import bs4 as bs
import os
import shutil
import boto3
import botocore
import logging

from pathlib import Path

LOGGER = logging.getLogger("src." + __name__)


class EmptyFileError(Exception):
    """ Exception for an empty table """
    pass


def load_and_parse_xml(url):
    """
    Loads, parse and retu
    :param url: the url that returns the xml data
    :return:
    """
    source = urllib.request.urlopen(url).read()
    soup = bs.BeautifulSoup(source, 'xml')  # TODO read about soup
    return soup


def save_s3_data(context, soup):
    # Clean the old data
    _clear_dir_files(context.s3_path_batch)
    _clear_dir_files(context.s3_path_sensors)

    # Get the sessions
    bucket_name, session = _get_bucket_session(context, soup)  # TODO read about bucket s3

    # Download data in iterations using the keys
    for process_key in soup.find_all('Key'):
        key = process_key.text

        s3 = session.resource('s3')
        output_file = context.s3_path_batch + os.path.sep + key
        if key != "sensor-data/":
            try:
                s3.Bucket(bucket_name).download_file(key, output_file)
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == "404":
                    print("The object does not exist.")
                else:
                    raise


def _clear_dir_files(dir_path):
    """
    Remove data including the dir and creates an empty directory
    :param dir_path:
    :return:
    """
    dir_path = Path(dir_path)
    if dir_path.exists() and dir_path.is_dir():
        shutil.rmtree(dir_path)
        pass
        # create empty directory
    os.mkdir(dir_path)


def read_parquet_data(context, file):
    """This method loads the data from the parquet files

        :param context: Application Context
        :param file: file whose data needs to be loaded
        :return: dataframe of the loaded table
        """

    df = context.spark.read.parquet(file)

    if len(df.head(1)) == 0:
        LOGGER.error(f'File {file} is found empty')
        raise EmptyFileError(f'Table {file} is found empty')

    LOGGER.info('Data is successfully loaded from table: %s', file)
    return df


def save_data_parquet(df, output_path, partition_key=None):
    """
    Saves the data
    :param df: the dataframe
    :param output_path: path where data needs to be stored
    :param partition_key: column on which partition needs to be done
    :return: None
    """
    if partition_key:
        df.repartition(partition_key).write.partitionBy(partition_key).parquet(output_path)
    else:
        df.write.parquet(output_path)


def save_as_csv(df, filename, coalesce=1):
    """
    save df as CSV
    :param df: pyspark df
    :param filename filename of the csv file
    :param coalesce: partition number
    :return: None
    """
    fil_dir = Path(filename)
    if fil_dir.exists() and fil_dir.is_dir():
        shutil.rmtree(filename)
        print('File existed, so removed it')

    df.coalesce(coalesce).write.csv(filename + ".csv", header='true')


def _get_bucket_session(context, soup):
    """
    returns the bucket session
    :param context: ApplicationContext
    :param soup: xml object
    :return:
    """
    bucket_name = soup.find('Name').text

    return bucket_name, boto3.Session(
        aws_access_key_id=context.aws_access_key_id,
        aws_secret_access_key=context.aws_secret_access_key
    )
