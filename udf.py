import logging.config
from pyspark.sql.functions import *
from pyspark.sql.types import *

logging.config.fileConfig('Properties/configuration/logging.config')

logger = logging.getLogger('UDF')

logger.warning("Calling the user defined function")
@udf(returnType=IntegerType())
def split_col(col):
    try:
        x = len(col.split(" "))
    except Exception as e:
        logger.error("error in the user defined module")
        raise
    else:
        logger.warning('executed the udf')
        return x

