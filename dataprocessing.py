import logging.config
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from udf import split_col

logging.config.fileConfig('Properties/configuration/logging.config')

logger = logging.getLogger('Dataprocessing')
logger.warning('starting the data processing module')

logger.warning("creating the class method for data standardization -cleaning, transformations etc")


class data_Clean_Transform:

    def distinct_presc(self,df_presc):

        try:
            logger.warning('invoking distinct_presc module ')
            df_distinct=df_presc.select('pres_npi','pres_city').distinct()\
                .groupby('pres_city').agg(count('*').alias('presc_count'))
        except Exception as e:
            logger.error('error in distinct prescriber module,traceback========>',str(e))

            raise
        else:
            logger.warning('distinct_presc module executed sucessfully')
            return df_distinct


    def col_sel(self, df1, df2):

        logger.warning("creating the method col_sel inside main class")
        try:
            logger.warning("selecting the requried columns from df1 dataset")
            city = df1.select(upper(col('city_ascii')).alias('city'), upper(col('state_id')).alias('state_id'),
                              upper(col('state_name')).alias('state_name'),
                              upper(col('county_name')).alias('countyname'), col('zips')).distinct()

            logger.warning("selected the requried columns from df1 dataset")
            logger.warning("selecting the requried columns from df2 dataset")

            presc = df2.select(col('npi').alias('pres_npi'),
                               col('nppes_provider_last_org_name').alias('pres_last_name'),
                               col('nppes_provider_first_name').alias('pres_first_name'),
                               col('nppes_provider_city').alias('pres_city'),
                               col('nppes_provider_state').alias('pres_state'),
                               col('specialty_description').alias('specialty_description'),
                               col('drug_name').alias('drug_name'), col('total_claim_count').alias('total_claim_count'),
                               col('total_day_supply').alias('total_day_supply'),
                               col('total_drug_cost').alias('total_drug_cost'),
                               col('years_of_exp').alias('years_of_exp')).withColumn('Country_Name',lit('USA'))\
                .withColumn('years_of_exp',regexp_replace(col('years_of_exp'),r"^= ",''))\
                .withColumn('years_of_exp',col('years_of_exp').cast(IntegerType()))\
                .withColumn('pres_fullname',concat_ws(" ",col('pres_first_name'),col('pres_last_name')))\
                .drop('pres_first_name','pres_last_name').dropna(subset=['pres_npi','drug_name']).distinct()

            logger.warning("selected the requried columns from df2 dataset")

        except Exception as e:

            logger.error(" error in method col sel of class data_Clean_Transforn, traceback======>", str(e))

            raise
        else:

            logger.warning("excecuted the module col sel sucessfully")

            return city, presc

    def distinct_presc(self,df_presc):

        try:
            logger.warning('invoking distinct_presc module ')
            df_distinct=df_presc.select('pres_npi','pres_city').distinct()\
                .groupby('pres_city').agg(count('*').alias('presc_count'))
        except Exception as e:
            logger.error('error in distinct prescriber module,traceback========>',str(e))

            raise
        else:
            logger.warning('distinct_presc module executed sucessfully')
            return df_distinct

    logger.warning("initalizing the txn count module by city")

    def txn_count(self,df_presc):
        logger.warning('initialzing the txn module')
        try:
            df_txn_count=df_presc.groupby('pres_city').agg(sum('total_claim_count').alias('txn_count'))
            df_presc1=df_presc.join(df_txn_count,[df_presc['pres_city']==df_txn_count['pres_city']],'inner').drop(df_txn_count.pres_city)
        except Exception as e:
            logger.error('error in txn_count module')
            raise
        else:
            logger.warning('module txn_count executed sucessfully')
            return df_presc1

    def pres_city_only(self,df_city,df_presc):
        logger.warning('invoking pres_city_module')
        try:
            logger.warning('applying the logic to select only data in pres if it has city assigned in dim')
            df_city_fil=df_city.select('city','state_id').distinct()
            df_pres_city=df_presc.join(broadcast(df_city_fil),
                                       [df_presc['pres_city']==df_city_fil['city'],
                                        df_presc['pres_state']==df_city_fil['state_id']],'inner')
            logger.warning('logic of pres_city_only module executed sucessfully')
        except Exception as e:
            logger.error('error in the module pres_city')
            raise
        else:
            logger.warning('Module pres_city executed sucessfully')
            return df_pres_city

    def zip_size(self,df_city):
        try:
            df_city=df_city.withColumn('zip_size',split_col(col('zips')))
        except Exception as e:
            logger.error('error in calculating the length of the zip,traceback=====>',str(e))
            raise
        else:
            logger.warning("calculated the length sucessfully")
            return df_city

    def top_txn_state(self,df):
        try:
            logger.warning("invoking the top_txn_state module ")
            win = Window.partitionBy("pres_state").orderBy(col('txn_count').desc())
            df_top5=df.select("*").filter((df.years_of_exp>=20) & (df.years_of_exp<=50))\
            .withColumn('Ranking',dense_rank().over(win)).filter(col('Ranking')<=5)
        except Exception as e:
            logger.error("error in top 5 transaction count module",str(e))
            raise
        else:
            logger.warning("top txn module executed sucessfully")
            return df_top5

