import os

import get_env_variables as gav
import logging.config

logging.config.fileConfig('Properties/configuration/logging.config')

logger = logging.getLogger('Ingest')
logger.warning("Ingesting the file")


def read_data(spark, file_path):
    try:
        logger.warning("inside the read_data module")

        for file in os.listdir(file_path):
            if file.endswith('.parquet'):
                path = file_path + '\\' + file
                logger.warning('reading the parquet files')
                df = spark.read.parquet(path)
                logger.warning('read the parquet files')
            elif file.endswith('.csv'):
                logger.warning('reading the csv files')
                path = file_path + '\\' + file
                df = spark.read.format('csv').option("header", True).load(path)
                logger.warning('read the csv files')
    except Exception as e:
        logger.error("error in the ingest module , tracee back=====", str(e))
        raise
    else:
        logger.warning('dataframe read successfully')
        return df


def validate_data(df):

    try:
        logger.warning('inside the validate module to get the count of data')

        df_c=df.count()

        logger.warning('got the count of data')
    except Exception as e:
        logger.error("Error in the validate method in read_data module traceback==========",str(e))
        raise
    else:
        logger.warning('Exceuted count succesfully ')
        return df_c

