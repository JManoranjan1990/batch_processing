
import logging.config
logging.config.fileConfig('Properties/configuration/logging.config')
loggers=logging.getLogger('Validate')
def gcd(spark):
    try:
        loggers.warning('Started the gcd module')
        d=spark.sql("select current_date")
        loggers.warning("validating the sparkobject with current date" ,str(d.collect()))

    except Exception as e:
        loggers.error('An error occured in GCD ',str(e))
        # print(e)
        raise

    else:
        loggers.warning("Validation done!!!")

