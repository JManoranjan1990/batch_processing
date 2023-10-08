
from pyspark.sql import SparkSession
import logging.config
logging.config.fileConfig('Properties/configuration/logging.config')
loggers=logging.getLogger('SparkSession')

def get_spark_obj(env, appName):

    try:
        loggers.info("get spark obj method started")
        if env == 'Dev':
            master = 'local'
        else:
            master = 'Yarn'

        loggers.info('master is {}'.format(master))

        spark = SparkSession.builder.master(master).appName(appName).getOrCreate()


    except Exception as exp:
        loggers.error('An error occured in spark obj creation',str(exp))
        raise
    else:
        loggers.info('Spark obj created')
        return spark
        # print(str(exp))


