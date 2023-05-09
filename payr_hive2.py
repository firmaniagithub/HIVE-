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

hv.sql("use payr")
hv.sql("CREATE TABLE IF NOT EXISTS mtd (trx_date string,subs_id string,lacci_id string,msisdn string,cust_type_desc string,cust_subtype_desc string,area_sales string,region_sales string,branch string,subbranch string,cluster_sales string,provinsi string,kabupaten string,kecamatan string,kelurahan string,node_type string,vol string,trx string,dur string,load_ts string,load_user string,event_date string,job_id string) STORED AS PARQUET; ")
hv.sql("load data local inpath 'file:///home/hdoop/d_input' overwrite into table mtd")

hv.sql("CREATE TABLE IF NOT EXISTS mm (mm_date string,subs_id string,lacci_id string,msisdn string,cust_type_desc string,cust_subtype_desc string,area_sales string,region_sales string,branch string,subbranch string,cluster string,provinsi string,kabupaten string,kecamatan string,kelurahan string,node_type string,vol string,trx string,dur string,load_ts string,load_user string,event_date string,job_id string) STORED AS PARQUET; ")
hv.sql("load data local inpath 'file:///home/hdoop/e_input' overwrite into table mm")

hv.sql("CREATE TABLE IF NOT EXISTS lacimma ( lac string,ci string,cell_name string,vendor string,site_id string,ne_id string,site_name_lacimma string,msc_name string,node string,longitude string,latitude string,region_network string,provinsi string,kabupaten string,kecamatan string,kelurahan string,region_sales string,branch string,subbranch string,address string,cluster_sales string,mitra_ad string,cgi_pre string,cgi_post string,ocs_cluster string,ocs_zone string,area_sales string,eci string,lacci_id_lacimma string,file_date string,file_id string,load_ts string,load_user string,event_date_lacimma string) \
    row format delimited fields terminated by ';'\
    ")
hv.sql("load data local inpath 'file:///home/hdoop/lacimma.csv' overwrite into table lacimma")

hv.sql("CREATE TABLE IF NOT EXISTS product (trx_date string,rkey string,mapping_key_type string,service_type string,l1_name string,l2_name string,l3_name string,l4_name string,price string,updated_date string,product_name string,plan_type string,plan_service string,quota_value string,quota_unit string,validity_value string,validity_unit string,vascode_type string,file_id string,load_ts string,load_user string,event_date string,product_category string,future_string_1 string,future_string_2 string,future_string_3 string) STORED AS PARQUET; ")
hv.sql("load data local inpath 'file:///home/hdoop/j_input' overwrite into table product")

payr1=hv.table('payr.PAYR_1_MIN')
mtd=hv.table('payr.mtd')
mm=hv.table('payr.mm')
lacimma=hv.table('payr.lacimma')
product_lime=hv.table('payr.product')

mtd=mtd.select('subs_id','lacci_id').withColumnRenamed("subs_id","subs_id_mtd").withColumnRenamed("lacci_id","lacci_id_mtd")
mtd.dropDuplicates()

join_mtd = payr1.join(mtd,payr1.subs_id ==  mtd.subs_id_mtd, how = 'left')
join_mtd=join_mtd.withColumn("trx_lacci_final",when(join_mtd.trx_lacci == "|",""))
join_mtd=join_mtd.withColumn("trx_fix" ,when((join_mtd.trx_lacci_final.isNull()) & (join_mtd.subs_id == join_mtd.subs_id_mtd),(join_mtd.lacci_id_mtd))\
        .otherwise(join_mtd.trx_lacci))\
        .select('payment_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','bundling_id','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_fix','retry_count','subs_id','cust_type_desc','cust_subtype_desc','subs_id_mtd','lacci_id_mtd')
join_mtd=join_mtd.withColumnRenamed("trx_fix","trx_lacci")

mm=mm.select('subs_id','lacci_id').withColumnRenamed("subs_id","subs_id_mm").withColumnRenamed("lacci_id","lacci_id_mm")
mm=mm.dropDuplicates(['subs_id_mm'])
join_mm = join_mtd.join(mm,join_mtd.subs_id ==  mm.subs_id_mm, how = 'left')
join_mm=join_mm.withColumn("trx_lacci_final",when(join_mtd.trx_lacci == "|",""))
join_mm=join_mm.withColumn("trx_fix" ,when((join_mm.trx_lacci_final.isNull()) & (join_mm.subs_id == join_mm.subs_id_mm),(join_mm.lacci_id_mm))\
        .otherwise(join_mm.trx_lacci)).select('payment_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','bundling_id','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_fix','retry_count','subs_id','cust_type_desc','cust_subtype_desc','subs_id_mtd','lacci_id_mtd','subs_id_mm','lacci_id_mm')
join_mm=join_mm.withColumnRenamed("trx_fix","trx_lacci")

join_mm1=join_mm.withColumn("lacci_id_flag",when((join_mm.subs_id == join_mm.subs_id_mtd) & (join_mm.lacci_id_mtd == join_mm.trx_lacci), "1#")\
        .when ((join_mm.subs_id == join_mm.subs_id_mm) & (join_mm.lacci_id_mm == join_mm.trx_lacci), "2#").otherwise(""))
join_mm1=join_mm1.withColumn("lacci_id",split(col("lacci_id_flag"), "#").getItem(1))\
        .withColumn("lacci_closing_flag",split(col("lacci_id_flag"), "#").getItem(0)).select('payment_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','bundling_id','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','retry_count','subs_id','cust_type_desc','cust_subtype_desc','lacci_id_flag','lacci_id','lacci_closing_flag')


lacimma=lacimma.select('lac','ci','node','provinsi','kabupaten','kecamatan','kelurahan','region_sales','branch','subbranch','address','cluster_sales','cgi_pre','cgi_post','ocs_cluster','area_sales','eci','lacci_id_lacimma')
lacimma=lacimma.dropDuplicates(['cgi_post'])
lacimma_mm1= join_mm1.join(lacimma,join_mm1.trx_lacci ==  lacimma.lacci_id_lacimma, how = 'left')
data_flag=lacimma_mm1.withColumn('Flag',when(col('trx_lacci') == col('lacci_id_lacimma'),'SAME').otherwise('DIFF'))

good1=data_flag.filter(col('Flag')  == 'SAME').select('payment_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','bundling_id','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','retry_count','subs_id','cust_type_desc','cust_subtype_desc','lac','ci','node','provinsi','kabupaten','kecamatan','kelurahan','region_sales','branch','subbranch','cluster_sales','area_sales','lacci_id_flag','lacci_id','lacci_closing_flag','lacci_id_lacimma','eci','cgi_post')
reject1=data_flag.filter(col('Flag') == 'DIFF').select('payment_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','bundling_id','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','retry_count','subs_id','cust_type_desc','cust_subtype_desc','lacci_id_flag','lacci_id','lacci_closing_flag')

lacimma_mm2= reject1.join(lacimma,reject1.trx_lacci ==  lacimma.eci, how = 'left')
data_flag2=lacimma_mm2.withColumn('Flag',when(col('trx_lacci') == col('eci'),'SAME').otherwise('DIFF'))

good2=data_flag2.filter(col('Flag')  == 'SAME')\
        .select('payment_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','bundling_id','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','retry_count','subs_id','cust_type_desc','cust_subtype_desc','lac','ci','node','provinsi','kabupaten','kecamatan','kelurahan','region_sales','branch','subbranch','cluster_sales','area_sales','lacci_id_flag','lacci_id','lacci_closing_flag','lacci_id_lacimma','eci','cgi_post')
reject2=data_flag2.filter(col('Flag') == 'DIFF')\
        .select('payment_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','bundling_id','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','retry_count','subs_id','cust_type_desc','cust_subtype_desc','lacci_id_flag','lacci_id','lacci_closing_flag')

lacimma_mm3= reject2.join(lacimma,reject2.trx_lacci ==  lacimma.cgi_post, how = 'left')
lacimma_mm3=lacimma_mm3.select('payment_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','bundling_id','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','retry_count','subs_id','cust_type_desc','cust_subtype_desc','lac','ci','node','provinsi','kabupaten','kecamatan','kelurahan','region_sales','branch','subbranch','cluster_sales','area_sales','lacci_id_flag','lacci_id','lacci_closing_flag','lacci_id_lacimma','eci','cgi_post')

from functools import reduce 
df = [ good1,good2,lacimma_mm3 ]
merge = reduce(DataFrame.unionAll, df)
merge =merge.dropDuplicates()
prefix = lit("0#")
lacci_id_flag = merge.withColumn("lacci_id_flag_2",when((merge.trx_lacci != "-99") & ((merge.trx_lacci == merge.lacci_id_lacimma)\
        | (merge.trx_lacci == merge.eci) | (merge.trx_lacci == merge.cgi_post))\
        & (merge.area_sales == "UNKNOWN") & (merge.lacci_id_flag.isNull()), (concat(prefix, merge.lacci_id_lacimma))) \
        .when((merge.trx_lacci != "-99") & ((merge.trx_lacci == merge.lacci_id_lacimma) | (merge.trx_lacci == merge.eci) | (merge.trx_lacci == merge.cgi_post))\
        & (merge.area_sales != "UNKNOWN") & (merge.lacci_id_flag.isNotNull()), (concat(merge.lacci_id_flag, merge.trx_lacci))).otherwise("-1#-99"))
closing_flag = lacci_id_flag.withColumn("lacci_id_final",split(col("lacci_id_flag_2"), "#").getItem(1))\
        .withColumn("lacci_closing_flag_final",split(col("lacci_id_flag_2"), "#").getItem(0))
closing_flag=closing_flag.select('payment_timestamp','trx_date','trx_hour','msisdn','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','offer_id','payment_channel','cell_id','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','pre_post_flag','event_date','bundling_id','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','retry_count','subs_id','cust_type_desc','cust_subtype_desc','lac','ci','node','provinsi','kabupaten','kecamatan','kelurahan','region_sales','branch','subbranch','cluster_sales','area_sales','lacci_id_flag_2','lacci_id_final','lacci_closing_flag_final')
closing_flag=closing_flag.withColumnRenamed("lacci_id_flag_2","lacci_id_flag").withColumnRenamed("lacci_id_final","lacci_id").withColumnRenamed("lacci_closing_flag_final","lacci_closing_flag") 

product_lime1=product_lime.withColumn("r_key",when ((product_lime.mapping_key_type.isNull()) | (product_lime.mapping_key_type == ("")) ,(product_lime.rkey))\
        .otherwise(product_lime.mapping_key_type))
product_lime1=product_lime1.select('trx_date','mapping_key_type','service_type','l1_name','l2_name','l3_name','l4_name','price','updated_date','product_name','plan_type','plan_service','quota_value','quota_unit','validity_value','validity_unit','vascode_type','file_id','load_ts','load_user','r_key')
product_lime1=product_lime1.withColumnRenamed("trx_date","trx_date_product")

mm_product=closing_flag.join(product_lime1,closing_flag.offer_id ==  product_lime1.r_key, how = 'left')
mm_product=mm_product.withColumn("file_id", lit("")).withColumn("load_ts", lit(20221219)).withColumn("load_user", lit("Solusi247"))\
        .select('payment_timestamp','trx_date','trx_hour','msisdn','subs_id','cust_type_desc','cust_subtype_desc','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','offer_id','l4_name','l3_name','l2_name','l1_name','payment_channel','cell_id','lacci_id','lacci_closing_flag','lac','ci','node','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','file_id','load_ts','load_user','pre_post_flag','event_date','bundling_id','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','lacci_id_flag','retry_count')
mm_product=mm_product.withColumn("status_reject", when((coalesce('subs_id','cust_type_desc','cust_subtype_desc','l4_name','l3_name','l2_name','l1_name','lacci_id','lacci_closing_flag','lac','ci','node','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan').isNull())\
        |(coalesce('subs_id','cust_type_desc','cust_subtype_desc','l4_name','l3_name','l2_name','l1_name','lacci_id','lacci_closing_flag','lac','ci','node','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan') == ("NQ"))\
        |(coalesce('subs_id','cust_type_desc','cust_subtype_desc','l4_name','l3_name','l2_name','l1_name','lacci_id','lacci_closing_flag','lac','ci','node','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan') == (-99)), ("YES")).otherwise("NO"))

PAYR_REJECT=mm_product.filter(col('status_reject')  == 'YES').select('payment_timestamp','trx_date','trx_hour','msisdn','subs_id','cust_type_desc','cust_subtype_desc','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','offer_id','l4_name','l3_name','l2_name','l1_name','payment_channel','cell_id','lacci_id','lacci_closing_flag','lac','ci','node','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','file_id','load_ts','load_user','pre_post_flag','event_date','bundling_id','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','lacci_id_flag','retry_count','status_reject')
PAYR_GOOD=mm_product.filter(col('status_reject')  == 'NO').select('payment_timestamp','trx_date','trx_hour','msisdn','subs_id','cust_type_desc','cust_subtype_desc','bss_order_id','plan_id','plan_name','topping_id','topping_name','plan_price','offer_id','l4_name','l3_name','l2_name','l1_name','payment_channel','cell_id','lacci_id','lacci_closing_flag','lac','ci','node','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','future_string_1','future_string_2','future_string_3','brand','site_name','file_id','load_ts','load_user','pre_post_flag','event_date','bundling_id','future_string_5','future_string_6','future_string_7','future_string_8','future_string_9','future_string_10','filename','trx_lacci','lacci_id_flag','retry_count','status_reject')

PAYR_GOOD.write.csv('file:///home/hdoop/spark-3.3.1-bin-hadoop3/PAYR_HIVE_GOOD',sep ='~')
PAYR_REJECT.write.csv('file:///home/hdoop/spark-3.3.1-bin-hadoop3/PAYR_HIVE_REJECT',sep ='~')
end_date = datetime.now() + timedelta(hours=7)
duration = (end_date - start_date).seconds
##
hv.sql("use payr")
hv.sql("drop table PAYR_GOOD")
hv.sql("drop table PAYR_REJECT")
hv.sql("CREATE TABLE IF NOT EXISTS PAYR_GOOD (payment_timestamp string,trx_date string,trx_hour string,msisdn string,subs_id string,cust_type_desc string,cust_subtype_desc string,bss_order_id string,plan_id string,plan_name string,topping_id string,topping_name string,plan_price string,offer_id string,l4_name string,l3_name string,l2_name string,l1_name string,payment_channel string,cell_id string,lacci_id string,lacci_closing_flag string,lac string,ci string,node_type string,area_sales string,region_sales string,branch string,subbranch string,cluster_sales string,provinsi string,kabupaten string,kecamatan string,kelurahan string,indicator_4g string,future_string_1 string,future_string_2 string,future_string_3 string,brand string,site_name string,file_id string,load_ts string,load_user string,pre_post_flag string,event_date string,bundling_id string,future_string_5 string,future_string_6 string,future_string_7 string,future_string_8 string,future_string_9 string,future_string_10 string)\
    row format delimited fields terminated by '~'\
    ")
hv.sql("CREATE TABLE IF NOT EXISTS PAYR_REJECT (payment_timestamp string,trx_date string,trx_hour string,msisdn string,subs_id string,cust_type_desc string,cust_subtype_desc string,bss_order_id string,plan_id string,plan_name string,topping_id string,topping_name string,plan_price string,offer_id string,l4_name string,l3_name string,l2_name string,l1_name string,payment_channel string,cell_id string,lacci_id string,lacci_closing_flag string,lac string,ci string,node_type string,area_sales string,region_sales string,branch string,subbranch string,cluster_sales string,provinsi string,kabupaten string,kecamatan string,kelurahan string,indicator_4g string,future_string_1 string,future_string_2 string,future_string_3 string,brand string,site_name string,file_id string,load_ts string,load_user string,pre_post_flag string,event_date string,bundling_id string,future_string_5 string,future_string_6 string,future_string_7 string,future_string_8 string,future_string_9 string,future_string_10 string,filename string,trx_lacci string,lacci_id_flag string,retry_count string,status_reject string)\
    row format delimited fields terminated by '~'\
    ")
hv.sql("load data local inpath 'file:///home/hdoop/spark-3.3.1-bin-hadoop3/PAYR_HIVE_GOOD' overwrite into table PAYR_GOOD")
hv.sql("load data local inpath 'file:///home/hdoop/spark-3.3.1-bin-hadoop3/PAYR_HIVE_REJECT' overwrite into table PAYR_REJECT")
print(start_date.strftime('%Y-%m-%d %H:%M:%S'))
print(end_date.strftime('%Y-%m-%d %H:%M:%S'))
print(duration)