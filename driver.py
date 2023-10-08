import os
import sys

import get_env_variables as gav
from sparksession import get_spark_obj
from validate import gcd
from Ingest import read_data, validate_data
from dataprocessing import data_Clean_Transform
import logging
import logging.config

logging.config.fileConfig('Properties/configuration/logging.config')


def main():
    global file_format
    try:
        logging.info('I am in the main method')
        logging.info('calling the sparkobject')
        spark = get_spark_obj(gav.env, gav.appName)
        logging.info('Spark obj created', spark)
        # print('Spark obj created',spark)
        logging.info('validating sparkobj')
        gcd(spark)
        logging.info(' invoking ingest module for olap module')
        df_city = read_data(spark=spark, file_path=gav.src_olap)
        df_city.show()
        logging.info("read the file from path {}".format(gav.src_olap))
        logging.info('calling count from validate method')
        count = validate_data(df_city)
        # print(count)
        logging.info("got the count sucessfully")

        logging.info(' invoking ingest module for oltp module')
        df_fact = read_data(spark=spark, file_path=gav.src_oltp)
        df_fact.show()
        logging.info("read the file from path {}".format(gav.src_oltp))
        logging.info('calling count from validate method')
        count = validate_data(df_fact)
        # print(count)
        logging.info("got the count sucessfully")

        logging.info('invoking the data processing -data clean transform module')

        data_process = data_Clean_Transform()

        logging.info('invoking the  select col method in clean transform module')

        city, presc = data_process.col_sel(df_city, df_fact)
        logging.info('Executed the select col method sucessfully')
        print(city.count(), presc.count())
        logging.info('printing the count of city and presc')
        city.show()
        presc.show()

        logging.info('invoking the distinct prescriber count by module for city')

        presc_countby_city=data_process.distinct_presc(presc)

        logging.info('printing the distinct prescriber count by module for city')

        presc_countby_city.show()

        logging.info('invoking the distinct txn count by module for city')
        txn_count=data_process.txn_count(presc)

        txn_count.show()
        logging.info('printing the distinct txn count by module for city')

        logging.info('invoking the distinct prescriber count by module for city')

        presc_countby_city = data_process.distinct_presc(presc)

        logging.info('printing the distinct prescriber count by module for city')

        presc_countby_city.show()

        logging.info('invoking the distinct txn count by module for city')
        txn_count = data_process.txn_count(presc)

        txn_count.show()
        logging.info('printing the distinct txn count by module for city')

        logging.info('invoking the pres_city_only')
        presc_city_only = data_process.pres_city_only(city, presc)
        presc_city_only.show()
        logging.info('printing the result of pres_city_only')

        logging.info('invoking the zip size module')
        df_city_size = data_process.zip_size(city)
        # df_city_size.show()
        logging.info('printing the result of df_city_size')

        logging.info('invoking the top txn count module')
        df_top5 = data_process.top_txn_state(txn_count)
        df_top5.show()

        df_top5.write.mode('overwrite').format('orc').save(gav.city_path)

        # logging.info( " top txn count exceuted successfuly ")



    except Exception as e:
        logging.error("An error occured while calling the main function, please check the trace ----" + str(e))
        sys.exit(1)


if __name__ == '__main__':
    main()

    logging.info("Application executed")
