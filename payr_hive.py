import os, sys, gc, ast
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import date_format
from datetime import *
from dateutil.relativedelta import relativedelta

from pyspark.sql import functions as F
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from os.path import abspath
from pyspark.sql import SparkSession
from configparser import ConfigParser
sys.path.insert(0, '/home/cdsw/')

#run_date = sys.argv[1]
run_date = '2023-05-03'
first_date_prev = (datetime.strptime(run_date,'%Y-%m-%d') - relativedelta(months=1)).strftime('%Y-%m-01')
last_date_prev = (datetime.strptime(first_date_prev,'%Y-%m-%d') + relativedelta(months=1) - timedelta(days=1)).strftime('%Y-%m-%d')
load_ts = (datetime.today() + timedelta(hours=7)).strftime('%Y-%m-%d %H:%M:%S')
load_date = (datetime.today() + timedelta(hours=7)).strftime('%Y-%m-%d')
start_date = datetime.now() + timedelta(hours=7)
# warehouse_location points to the default location for managed databases and tables
warehouse_location = abspath('spark-warehouse')

# Create spark session with hive enabled
spark = SparkSession \
    .builder \
    .appName("SparkByExamples.com") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", "thrift://192.168.56.105:9083") \
    .enableHiveSupport() \
    .getOrCreate()
hv = HiveContext(spark)
hv.sql("create database IF NOT EXISTS payr")
hv.sql("use payr")
hv.sql("drop table source_payr")
hv.sql("CREATE TABLE IF NOT EXISTS source_payr (payment_timestamp string,MSISDN_1 string,bss_order_id string,plan_id string,plan_name string,topping_id string,topping_name string,plan_price string,payment_channel string,cell_id string,indicator_4g string,future_string_1 string,future_string_2 string,future_string_3 string,future_string_4 string,future_string_5 string,future_string_6 string,future_string_7 string,future_string_8 string,future_string_9 string,future_string_10 string)\
        row format delimited fields terminated by '|'\
        ")    
hv.sql("load data local inpath 'file:///home/hdoop/PAYR_JKT_Pre_20221219235959_00000574_51.1.dat' overwrite into table source_payr")
hv.sql("CREATE TABLE IF NOT EXISTS subs_dim (subs_id string,msisdn string,price_plan_id string,area_hlr string,region_hlr string,cust_type_desc string,cust_subtype_desc string,customer_sub_segment string,city_hlr string) STORED AS PARQUET; ")
hv.sql("load data local inpath 'file:///home/hdoop/subs' overwrite into table subs_dim")
####
sa_payr = hv.table('payr.source_payr')
sa_payr= sa_payr.withColumnRenamed("future_string_4","bundling_id")
proses1 =sa_payr.withColumn('trx_date', substring('payment_timestamp', 0,10)).withColumn('trx_hour', substring('payment_timestamp', 11,8))
proses1 =proses1.withColumn('Plan_price' ,when(proses1.plan_price == "NQ","").when(proses1.plan_price.isNull() ,"").otherwise(proses1.plan_price))

proses1 =proses1.withColumn('offer_id',when(proses1.plan_id == "SA10359",(proses1.future_string_1)).when(proses1.plan_id.isNull() ,(proses1.topping_id)).when(proses1.topping_id.isNull() ,(proses1.plan_id)).otherwise(proses1.plan_id))\
    .withColumn('event_date', substring('payment_timestamp', 0,10))\
    .withColumn("brand", lit("byU"))\
    .withColumn("site_name", lit("JKT"))\
    .withColumn("pre_post_flag", lit("1"))
    
proses2=proses1.withColumn('offer1', substring('cell_id', 10,7))
proses2=proses2.withColumn('final_offer_1',F.regexp_replace('offer1', r'^0', ''))

proses2=proses2.withColumn('offer2',F.expr("substring(cell_id,17,length(cell_id))"))
proses2=proses2.withColumn('final_offer_2',F.regexp_replace('offer2', r'^0', ''))

proses2=proses2.withColumn('offer3', substring('cell_id', 5,5))
proses2=proses2.withColumn('final_offer_3',F.regexp_replace('offer3', r'^0', ''))

proses2=proses2.withColumn('offer4',F.expr("substring(cell_id,10,length(cell_id))"))
proses2=proses2.withColumn('final_offer_4',F.regexp_replace('offer4', r'^0', ''))

proses3 =proses2.withColumn('concat_1', 
                    F.concat(F.col('final_offer_1'),F.lit('_'), F.col('final_offer_2')))
proses3 =proses3.withColumn('concat_2', 
                    F.concat(F.col('final_offer_3'),F.lit('_'), F.col('final_offer_4')))


proses4 = proses3.withColumn("final_cell_id",when(proses3.indicator_4g == 129,(proses3.concat_1))\
    .when(proses3.indicator_4g == 130,(proses3.concat_1))
    .when(proses3.indicator_4g == 128,(proses3.cell_id))\
    .when(proses3.indicator_4g == 131,(proses3.cell_id))\
    .otherwise(proses3.concat_2))

proses5 = proses4.withColumn("Cell1",F.regexp_replace('concat_1', r'[_]', '|'))
proses5 = proses5.withColumn("Cell2",F.regexp_replace('concat_2', r'[_]', '|'))\
            .withColumn("filename", lit("PAYR_JKT_Pre_20221219235959_00000574_51.1.dat"))

proses6 = proses5.withColumn("trx_lacci",when(proses5.indicator_4g == 129,(proses5.Cell1))\
    .when(proses5.indicator_4g == 130,(proses5.Cell1))
    .when(proses5.indicator_4g == 128,(proses5.cell_id))\
    .when(proses5.indicator_4g == 131,(proses5.cell_id))\
    .otherwise(proses5.Cell2)).withColumn("retry_count", lit(0)).select('payment_timestamp','trx_date','trx_hour','MSISDN_1','bss_order_id','plan_id','plan_name','topping_id','topping_name','Plan_price','offer_id','payment_channel','final_cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','bundling_id','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','retry_count').withColumnRenamed("final_cell_id","cell_id")

subs_dim=hv.table('payr.subs_dim')
subs_dim=subs_dim.select('subs_id','msisdn','cust_type_desc','cust_subtype_desc')

subs_payr = proses6.join(subs_dim,proses6.MSISDN_1 ==  subs_dim.msisdn, how = 'left')

subs_payr1 = subs_payr.fillna("-99",["subs_id"]).fillna("UNKNOWN",["cust_type_desc"]).fillna("UNKNOWN",["cust_subtype_desc"]).select('payment_timestamp','trx_date','trx_hour','MSISDN_1','bss_order_id','plan_id','plan_name','topping_id','topping_name','Plan_price','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','bundling_id','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','retry_count','subs_id','cust_type_desc','cust_subtype_desc')
subs_payr1.show(5, truncate=False)
subs_payr1.write.csv('file:///home/hdoop/spark-3.3.1-bin-hadoop3/PAYR_HIVE_1',sep ='~')

end_date = datetime.now() + timedelta(hours=7)
duration = (end_date - start_date).seconds
####
hv.sql("use payr")
hv.sql("drop table PAYR_1")
hv.sql("CREATE TABLE IF NOT EXISTS PAYR_1 (payment_timestamp string,trx_date string,trx_hour string,msisdn string,bss_order_id string,plan_id string,plan_name string,topping_id string,topping_name string,plan_price string,offer_id string,payment_channel string,cell_id string,indicator_4g string,future_string_1 string,future_string_2 string,future_string_3 string,brand string,site_name string,pre_post_flag string,event_date string,bundling_id string,future_string_5 string,future_string_6 string,future_string_7 string,future_string_8 string,future_string_9 string,future_string_10 string,filename string,trx_lacci string,retry_count string,subs_id string,cust_type_desc string,cust_subtype_desc string)\
    row format delimited fields terminated by '~'\
    ")
hv.sql("load data local inpath 'file:///home/hdoop/spark-3.3.1-bin-hadoop3/PAYR_HIVE_1' overwrite into table PAYR_1")
print(start_date.strftime('%Y-%m-%d %H:%M:%S'))
print(end_date.strftime('%Y-%m-%d %H:%M:%S'))
print(duration)