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
from pyspark.sql import Window

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

hv.sql("drop database RCG_1 CASCADE")
hv.sql("create database IF NOT EXISTS RCG_1")
hv.sql("use RCG_1")
hv.sql("CREATE TABLE IF NOT EXISTS rnr (timestamp string,msisdn string,account string,recharge_channel string,expiration_date string,serial_number string,deltabalance string,balance_amount string,credit_indicator string,recharge_method string,recharge_id string,bonus_information string,providerid string,sourceip string,userid string,resultcode string,bank_code string,a_number_location string,balance_before string,adjustment_reason string,caseid string,crmuserid string,old_expiration_date string,split_code string,recharge_amount string,future_string_2 string,future_string_3 string,future_string_1 string,indicator_4g string)\
    row format delimited fields terminated by '|'\
    ")
hv.sql("load data local inpath 'file:///home/hdoop/20230222' overwrite into table rnr")
hv.sql("CREATE TABLE IF NOT EXISTS subs_dim (trx_date string,subs_id string,msisdn string,account_id string,status string,pre_post_flag string,activation_date string,deactivation_date string,los string,price_plan_id string,prefix string,area_hlr string,region_hlr string,city_hlr string,cust_type_desc string,cust_subtype_desc string,customer_sub_segment string,load_ts string,load_user string,job_id string,migration_date string)\
    STORED AS PARQUET;\
    ")
spark.read.parquet('file:///home/hdoop/CHG_REGULER/SUBS').write.mode('overwrite').saveAsTable("RCG_1.subs_dim")
hv.sql("CREATE TABLE IF NOT EXISTS mtd (trx_date string,subs_id string,lacci_id string,msisdn string,cust_type_desc string,cust_subtype_desc string,area_sales string,region_sales string,branch string,subbranch string,cluster_sales string,provinsi string,kabupaten string,kecamatan string,kelurahan string,node_type string,trx string,dur string,load_ts string,load_user string,event_date string,job_id string,lacci_area_name string,lacci_region_network string,lacci_cluster_sales string,lacci_lac string,lacci_ci string,lacci_node string,lacci_region_sales string,lacci_branch string,lacci_subbranch string,lacci_provinsi string,lacci_kabupaten string,lacci_kecamatan string,lacci_kelurahan string)\
    STORED AS PARQUET;\
    ")
spark.read.parquet('file:///home/hdoop/CHG_REGULER/MTD/*/*').write.mode('overwrite').saveAsTable("RCG_1.mtd")
hv.sql("CREATE TABLE IF NOT EXISTS mm (mm_date string,subs_id string,lacci_id string,msisdn string,cust_type_desc string,cust_subtype_desc string,area_sales string,region_sales string,branch string,subbranch string,cluster_sales string,provinsi string,kabupaten string,kecamatan string,kelurahan string,node_type string,trx string,dur string,load_ts string,load_user string,event_date string,job_id string,lacci_area_name string,lacci_region_network string,lacci_cluster_sales string,lacci_lac string,lacci_ci string,lacci_node string,lacci_region_sales string,lacci_branch string,lacci_subbranch string,lacci_provinsi string,lacci_kabupaten string,lacci_kecamatan string,lacci_kelurahan string)\
    STORED AS PARQUET;\
    ")
hv.sql("load data local inpath 'file:///home/hdoop/CHG_REGULER/MM/*/*' overwrite into table mm")
hv.sql("CREATE TABLE IF NOT EXISTS lacimma (lac_1 string,ci_1 string,cell_name_1 string,vendor_1 string,site_id_1 string,ne_id_1 string,site_name_1 string,msc_name_1 string,node_1 string,longitude_1 string,latitude_1 string,region_network_1 string,provinsi_1 string,kabupaten_1 string,kecamatan_1 string,kelurahan_1 string,region_sales_1 string,branch_1 string,subbranch_1 string,address_1 string,cluster_sales_1 string,mitra_ad_1 string,cgi_pre_1 string,cgi_post_1 string,ocs_cluster_1 string,ocs_zone_1 string,area_sales_1 string,eci_1 string,lacci_id_1 string,file_date_1 string,file_id_1 string,load_ts_1 string,load_user_1 string,event_date_1 string)\
       row format delimited fields terminated by ';'\
    ")
hv.sql("load data local inpath 'file:///home/hdoop/CHG_REGULER/LACCIMA_DIM.csv' overwrite into table lacimma")
####
rnr=hv.table('RCG_1.rnr')\
        .withColumn("trim_timestamp", trim(F.col('timestamp')))\
        .withColumn("trimmsi", trim(F.col('msisdn')))\
        .withColumn("trimbalance", trim(F.col('deltabalance')))\
        .withColumn("trim_balance_ammount", trim(F.col('balance_amount')))\
        .withColumn('filter',when(F.col('trim_timestamp').isNotNull() & F.col('trimbalance').isNotNull() & F.col('trimmsi').isNotNull(), ("YES")).otherwise ("NO"))
RNR_REJECT=rnr.filter(col('filter') == "NO")
RNR_GOOD=rnr.filter(col('filter') == "YES")
list1=[129,130]
list2=[128,131]
proses1=RNR_GOOD.withColumn("timestamp_r", F.to_timestamp("timestamp",'yyyyMMddHHmmss'))\
        .withColumn('rundate', lit("20230120")).withColumn("trx_date", F.to_date("timestamp", "yyyyMMddHHmmss"))\
        .withColumn("trx_hour", F.hour("timestamp_r"))\
        .withColumn("brand", lit('ByU'))\
        .withColumn("rec_id",row_number().over(Window.orderBy(monotonically_increasing_id())))\
        .withColumn('filename', lit('file:/home/hdoop/20230222/'))\
        .withColumn('sitename', lit('file:/home/hdoop/20230222/'))\
        .withColumn('status_data', lit('REJECT'))\
        .withColumn('status', lit('NODUP'))\
        .withColumn("trim_indikator", trim(RNR_GOOD.indicator_4g))\
        .withColumn("trim_number", trim(RNR_GOOD.a_number_location))\
        .withColumn('subs1', substring('trim_number', 11,7))\
        .withColumn('subs2', substring('trim_number', 18,3))\
        .withColumn('subs3', substring('trim_number', 6,5))\
        .withColumn('subs4', substring('trim_number', 11,5))\
        .withColumn('retry_count', lit(0))\
        .withColumn('concat1', 
                    F.concat(F.col('subs1'),F.lit('|'), F.col('subs2')))\
        .withColumn('concat1',F.regexp_replace('concat1', r'^0+', ''))\
        .withColumn('concat2', 
                    F.concat(F.col('subs3'),F.lit('|'), F.col('subs4')))\
        .withColumn('concat2',F.regexp_replace('concat2', r'^0+', ''))\
        .withColumn('trx_lacci', when((F.col('trim_indikator') == '129') | (F.col('trim_indikator') == "130"),F.col('concat1')\
                    .when((F.col('trim_indikator') == '128') | (F.col('trim_indikator') == "131"), (F.col('a_number_location')))\
                    .otherwise(F.col('concat2'))))

SUBS=hv.table('RCG_1.subs_dim')\
        .withColumnRenamed("msisdn","msisdn_1").withColumnRenamed("trx_date","trx_date_1")\
        .withColumnRenamed("status","status_1")
        
join_sub = proses1.join(SUBS,proses1.msisdn ==  SUBS.msisdn_1, how = 'left')\
        .withColumn("subs_id", when((F.col('msisdn') == F.col('msisdn_1')), (F.col('subs_id'))).otherwise("-99"))\
        .withColumn("cust_type_desc", when((F.col('msisdn') == F.col('msisdn_1')), (F.col('cust_type_desc'))).otherwise("NQ"))\
        .withColumn("cust_subtype_desc", when((F.col('msisdn') == F.col('msisdn_1')), (F.col('cust_subtype_desc'))).otherwise("NQ"))\
        .withColumn('flag_status', lit('NODUP'))
        
MTD=hv.table('RCG_1.mtd')\
        .withColumnRenamed("trx_date","trx_date_2").withColumnRenamed("subs_id","subs_id_2").withColumnRenamed("msisdn","msisdn_2")\
        .withColumnRenamed("lacci_id","lacci_id_mtd").withColumnRenamed("cust_type_desc","cust_type_desc_2").withColumnRenamed("cust_subtype_desc","cust_subtype_desc_2")\
        .withColumnRenamed("load_ts","load_ts_1").withColumnRenamed("load_user","load_user_1")

JOIN_MTD=join_sub.join(MTD,(join_sub.subs_id ==  MTD.subs_id_2) & (join_sub.msisdn ==  MTD.msisdn_2), how = 'left')
proses3=JOIN_MTD.withColumn("mtd_lacci_id", when((JOIN_MTD.subs_id == JOIN_MTD.subs_id_2), (JOIN_MTD.lacci_id_mtd)))\
        .withColumn("mtd_area_name", when((JOIN_MTD.subs_id == JOIN_MTD.subs_id_2), (JOIN_MTD.lacci_area_name))).fillna("UNKNOWN",["mtd_area_name"])\
        .withColumn("mtd_regio", when((JOIN_MTD.subs_id == JOIN_MTD.subs_id_2), (JOIN_MTD.lacci_region_network)))\
        .withColumn("mtd_cluster_sales", when((JOIN_MTD.subs_id == JOIN_MTD.subs_id_2), (JOIN_MTD.lacci_cluster_sales)))\
        .withColumn("mtd_lac", when((JOIN_MTD.subs_id == JOIN_MTD.subs_id_2), (JOIN_MTD.lacci_lac)))\
        .withColumn("mtd_ci", when((JOIN_MTD.subs_id == JOIN_MTD.subs_id_2), (JOIN_MTD.lacci_ci)))\
        .withColumn("mtd_node", when((JOIN_MTD.subs_id == JOIN_MTD.subs_id_2), (JOIN_MTD.lacci_node)))\
        .withColumn("mtd_region_sales", when((JOIN_MTD.subs_id == JOIN_MTD.subs_id_2), (JOIN_MTD.lacci_region_sales)))\
        .withColumn("mtd_branch", when((JOIN_MTD.subs_id == JOIN_MTD.subs_id_2), (JOIN_MTD.lacci_branch)))\
        .withColumn("mtd_subbranch", when((JOIN_MTD.subs_id == JOIN_MTD.subs_id_2), (JOIN_MTD.lacci_subbranch)))\
        .withColumn("mtd_provinsi", when((JOIN_MTD.subs_id == JOIN_MTD.subs_id_2), (JOIN_MTD.lacci_provinsi)))\
        .withColumn("mtd_kabupaten", when((JOIN_MTD.subs_id == JOIN_MTD.subs_id_2), (JOIN_MTD.lacci_kabupaten)))\
        .withColumn("mtd_kecamatan", when((JOIN_MTD.subs_id == JOIN_MTD.subs_id_2), (JOIN_MTD.lacci_kecamatan)))\
        .withColumn("mtd_kelurahan", when((JOIN_MTD.subs_id == JOIN_MTD.subs_id_2), (JOIN_MTD.lacci_kelurahan)))\
        .withColumn("mtd_flag", when((JOIN_MTD.subs_id == JOIN_MTD.subs_id_2), ("1")).otherwise("0"))\
        .select('timestamp_r','trx_hour','trx_date','msisdn','subs_id','cust_type_desc','cust_subtype_desc','account','recharge_channel','expiration_date','serial_number','deltabalance','balance_amount','credit_indicator','recharge_method','recharge_id','bonus_information','providerid','sourceip','userid','resultcode','bank_code','a_number_location','indicator_4g','balance_before','adjustment_reason','caseid','crmuserid','old_expiration_date','split_code','recharge_amount','future_string_1','future_string_2','future_string_3','brand','load_ts','load_user','sitename','event_date','filename','rec_id','retry_count','status_data','status','trx_lacci','mtd_lacci_id','mtd_area_name','mtd_regio','mtd_cluster_sales','mtd_lac','mtd_ci','mtd_node','mtd_region_sales','mtd_branch','mtd_subbranch','mtd_provinsi','mtd_kabupaten','mtd_kecamatan','mtd_kelurahan','mtd_flag','flag_status')

MM=hv.table('RCG_1.mm')\
    .select('mm_date','subs_id','lacci_id','msisdn','cust_type_desc','cust_subtype_desc','lacci_area_name','lacci_region_network','lacci_cluster_sales','lacci_lac','lacci_ci','lacci_node','lacci_region_sales','lacci_branch','lacci_subbranch','lacci_provinsi','lacci_kabupaten','lacci_kecamatan','lacci_kelurahan')\
    .withColumnRenamed("subs_id","subs_id_3").withColumnRenamed("msisdn","msisdn_3")\
    .withColumnRenamed("lacci_id","lacci_id_mm").withColumnRenamed("cust_type_desc","cust_type_desc_3").withColumnRenamed("cust_subtype_desc","cust_subtype_desc_3")

JOIN_MM=proses3.join(MM,(proses3.subs_id ==  MM.subs_id_3) & (proses3.msisdn ==  MM.msisdn_3), how = 'left')\
    .withColumn("prefix", lit("a"))\
    .withColumn("counter",row_number().over(Window.orderBy(monotonically_increasing_id())))

proses4=JOIN_MM.withColumn("trx_lacci", when ((JOIN_MM.trx_lacci == "|") | (JOIN_MM.trx_lacci.isNull()),(concat(JOIN_MM.prefix, JOIN_MM.counter))).otherwise(JOIN_MM.trx_lacci))\
        .withColumn("mm_lacci_id", when((JOIN_MM.subs_id == JOIN_MM.subs_id_3), (JOIN_MM.lacci_id_mm)))\
        .withColumn("mm_area_name", when((JOIN_MM.subs_id == JOIN_MM.subs_id_3), (JOIN_MM.lacci_area_name))).fillna("UNKNOWN",["mm_area_name"])\
        .withColumn("mm_region_network", when((JOIN_MM.subs_id == JOIN_MM.subs_id_3), (JOIN_MM.lacci_region_network)))\
        .withColumn("mm_cluster_sales", when((JOIN_MM.subs_id == JOIN_MM.subs_id_3), (JOIN_MM.lacci_cluster_sales)))\
        .withColumn("mm_lac", when((JOIN_MM.subs_id == JOIN_MM.subs_id_3), (JOIN_MM.lacci_lac)))\
        .withColumn("mm_ci", when((JOIN_MM.subs_id == JOIN_MM.subs_id_3), (JOIN_MM.lacci_ci)))\
        .withColumn("mm_node", when((JOIN_MM.subs_id == JOIN_MM.subs_id_3), (JOIN_MM.lacci_node)))\
        .withColumn("mm_region_sales", when((JOIN_MM.subs_id == JOIN_MM.subs_id_3), (JOIN_MM.lacci_region_sales)))\
        .withColumn("mm_branch", when((JOIN_MM.subs_id == JOIN_MM.subs_id_3), (JOIN_MM.lacci_branch)))\
        .withColumn("mm_subbranch", when((JOIN_MM.subs_id == JOIN_MM.subs_id_3), (JOIN_MM.lacci_subbranch)))\
        .withColumn("mm_provinsi", when((JOIN_MM.subs_id == JOIN_MM.subs_id_3), (JOIN_MM.lacci_provinsi)))\
        .withColumn("mm_kabupaten", when((JOIN_MM.subs_id == JOIN_MM.subs_id_3), (JOIN_MM.lacci_kabupaten)))\
        .withColumn("mm_kecamatan", when((JOIN_MM.subs_id == JOIN_MM.subs_id_3), (JOIN_MM.lacci_kecamatan)))\
        .withColumn("mm_kelurahan", when((JOIN_MM.subs_id == JOIN_MM.subs_id_3), (JOIN_MM.lacci_kelurahan)))\
        .withColumn("mm_flag", when((JOIN_MM.subs_id == JOIN_MM.subs_id_3), ("1")).otherwise("0"))\
        .withColumn("mm_status", when((F.col('indicator_4g') == '129') | (F.col('indicator_4g') == "130"), "KONDISI1" )\
                .when((F.col('indicator_4g') == '128') | (F.col('indicator_4g') == "131"), "KONDISI2" ).otherwise("REJECT"))

STATUS_GOOD1=proses4.filter(col('mm_status')  == 'KONDISI1')
STATUS_GOOD2=proses4.filter(col('mm_status')  == 'KONDISI2')
STATUS_reject=proses4.filter(col('mm_status')  == 'REJECT')

LACIMMA=hv.table('RCG_1.lacimma')
JOIN_ECI=STATUS_GOOD1.join(LACIMMA,STATUS_GOOD1.trx_lacci ==  LACIMMA.eci_1, how = 'left')
JOIN_ECI=JOIN_ECI.withColumn('lacci_id', when((JOIN_ECI.trx_lacci == JOIN_ECI.eci_1) & (JOIN_ECI.area_sales_1 != "UNKNOWN"), (JOIN_ECI.lacci_id_1))\
                .when((JOIN_ECI.mtd_flag == "1") & (JOIN_ECI.mtd_area_name != "UNKNOWN"), (JOIN_ECI.mtd_lacci_id))\
                .when((JOIN_ECI.mm_flag == "1") & (JOIN_ECI.mm_area_name != "UNKNOWN"), (JOIN_ECI.mm_lacci_id)).otherwise("-99"))\
        .withColumn('lacci_closing_flag', when((JOIN_ECI.trx_lacci == JOIN_ECI.eci_1) & (JOIN_ECI.area_sales_1 != "UNKNOWN"), ("0"))\
                .when((JOIN_ECI.mtd_flag == "1") & (JOIN_ECI.mtd_area_name != "UNKNOWN"), ("1"))\
                .when((JOIN_ECI.mm_flag == "1") & (JOIN_ECI.mm_area_name != "UNKNOWN"), ("2")).otherwise("-1") )\
        .withColumn('lac', when((JOIN_ECI.trx_lacci == JOIN_ECI.eci_1), (JOIN_ECI.lac_1))\
                .when((JOIN_ECI.mtd_flag == "1"), (JOIN_ECI.mtd_lac))\
                .when((JOIN_ECI.mm_flag == "1"), (JOIN_ECI.mm_lac)).otherwise("NQ"))\
        .withColumn('ci', when((JOIN_ECI.trx_lacci == JOIN_ECI.eci_1), (JOIN_ECI.ci_1))\
                .when((JOIN_ECI.mtd_flag == "1"), (JOIN_ECI.mtd_ci))\
                .when((JOIN_ECI.mm_flag == "1"), (JOIN_ECI.mm_ci)).otherwise("NQ"))\
        .withColumn('node_type', when((JOIN_ECI.trx_lacci == JOIN_ECI.eci_1), (JOIN_ECI.node_1))\
                .when((JOIN_ECI.mtd_flag == "1"), (JOIN_ECI.mtd_node))\
                .when((JOIN_ECI.mm_flag == "1"), (JOIN_ECI.mm_node)).otherwise("NQ"))\
        .withColumn('area_sales', when((JOIN_ECI.trx_lacci == JOIN_ECI.eci_1), (JOIN_ECI.area_sales_1))\
                .when((JOIN_ECI.mtd_flag == "1"), (JOIN_ECI.mtd_area_name))\
                .when((JOIN_ECI.mm_flag == "1"), (JOIN_ECI.mm_area_name)).otherwise("NQ"))\
        .withColumn('region_sales', when((JOIN_ECI.trx_lacci == JOIN_ECI.eci_1), (JOIN_ECI.region_sales_1))\
                .when((JOIN_ECI.mtd_flag == "1"), (JOIN_ECI.mtd_region_sales))\
                .when((JOIN_ECI.mm_flag == "1"), (JOIN_ECI.mm_region_sales)).otherwise("NQ"))\
        .withColumn('branch', when((JOIN_ECI.trx_lacci == JOIN_ECI.eci_1), (JOIN_ECI.branch_1))\
                .when((JOIN_ECI.mtd_flag == "1"), (JOIN_ECI.mtd_branch))\
                .when((JOIN_ECI.mm_flag == "1"), (JOIN_ECI.mm_branch)).otherwise("NQ"))\
        .withColumn('subbranch', when((JOIN_ECI.trx_lacci == JOIN_ECI.eci_1), (JOIN_ECI.subbranch_1))\
                .when((JOIN_ECI.mtd_flag == "1"), (JOIN_ECI.mtd_subbranch))\
                .when((JOIN_ECI.mm_flag == "1"), (JOIN_ECI.mm_subbranch)).otherwise("NQ"))\
        .withColumn('cluster_sales', when((JOIN_ECI.trx_lacci == JOIN_ECI.eci_1), (JOIN_ECI.cluster_sales_1))\
                .when((JOIN_ECI.mtd_flag == "1"), (JOIN_ECI.mtd_cluster_sales))\
                .when((JOIN_ECI.mm_flag == "1"), (JOIN_ECI.mm_cluster_sales)).otherwise("NQ"))\
        .withColumn('provinsi', when((JOIN_ECI.trx_lacci == JOIN_ECI.eci_1), (JOIN_ECI.provinsi_1))\
                .when((JOIN_ECI.mtd_flag == "1"), (JOIN_ECI.mtd_provinsi))\
                .when((JOIN_ECI.mm_flag == "1"), (JOIN_ECI.mm_provinsi)).otherwise("NQ"))\
        .withColumn('kabupaten', when((JOIN_ECI.trx_lacci == JOIN_ECI.eci_1), (JOIN_ECI.kabupaten_1))\
                .when((JOIN_ECI.mtd_flag == "1"), (JOIN_ECI.mtd_kabupaten))\
                .when((JOIN_ECI.mm_flag == "1"), (JOIN_ECI.mm_kabupaten)).otherwise("NQ"))\
        .withColumn('kecamatan', when((JOIN_ECI.trx_lacci == JOIN_ECI.eci_1), (JOIN_ECI.kecamatan_1))\
                .when((JOIN_ECI.mtd_flag == "1"), (JOIN_ECI.mtd_kecamatan))\
                .when((JOIN_ECI.mm_flag == "1"), (JOIN_ECI.mm_kecamatan)).otherwise("NQ"))\
        .withColumn('kelurahan', when((JOIN_ECI.trx_lacci == JOIN_ECI.eci_1), (JOIN_ECI.kelurahan_1))\
                .when((JOIN_ECI.mtd_flag == "1"), (JOIN_ECI.mtd_kelurahan))\
                .when((JOIN_ECI.mm_flag == "1"), (JOIN_ECI.mm_kelurahan)).otherwise("NQ"))
LACIMMA1=JOIN_ECI.select('timestamp_r','trx_hour','trx_date','msisdn','subs_id','cust_type_desc','cust_subtype_desc','account','recharge_channel','expiration_date','serial_number','deltabalance','balance_amount','credit_indicator','recharge_method','recharge_id','bonus_information','providerid','sourceip','userid','resultcode','bank_code','a_number_location','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','balance_before','adjustment_reason','caseid','crmuserid','old_expiration_date','split_code','recharge_amount','future_string_1','future_string_2','future_string_3','brand','load_ts','load_user','sitename','event_date','filename','rec_id','retry_count','status_data','status','trx_lacci','flag_status')
#LACIMMA1=JOIN_ECI.withColumn('status_eci', when((JOIN_ECI.trx_lacci ==  JOIN_ECI.eci_1), "GOOD").otherwise("REJECT"))
#LACIMMA1=LACIMMA1.select('eci_1')

JOIN_CGI=STATUS_GOOD2.join(LACIMMA,STATUS_GOOD1.trx_lacci ==  LACIMMA.cgi_post_1, how = 'left')
JOIN_CGI=JOIN_CGI.withColumn('lacci_id', when((JOIN_CGI.trx_lacci == JOIN_CGI.cgi_post_1) & (JOIN_CGI.area_sales_1 != "UNKNOWN"), (JOIN_CGI.lacci_id_1))\
                .when((JOIN_CGI.mtd_flag == "1") & (JOIN_CGI.mtd_area_name != "UNKNOWN"), (JOIN_CGI.mtd_lacci_id))\
                .when((JOIN_CGI.mm_flag == "1") & (JOIN_CGI.mm_area_name != "UNKNOWN"), (JOIN_CGI.mm_lacci_id)).otherwise("-99"))\
        .withColumn('lacci_closing_flag', when((JOIN_CGI.trx_lacci == JOIN_CGI.cgi_post_1) & (JOIN_CGI.area_sales_1 != "UNKNOWN"), ("0"))\
                .when((JOIN_CGI.mtd_flag == "1") & (JOIN_CGI.mtd_area_name != "UNKNOWN"), ("1"))\
                .when((JOIN_CGI.mm_flag == "1") & (JOIN_CGI.mm_area_name != "UNKNOWN"), ("2")).otherwise("-1") )\
        .withColumn('lac', when((JOIN_CGI.trx_lacci == JOIN_CGI.cgi_post_1), (JOIN_CGI.lac_1))\
                .when((JOIN_CGI.mtd_flag == "1"), (JOIN_CGI.mtd_lac))\
                .when((JOIN_CGI.mm_flag == "1"), (JOIN_CGI.mm_lac)).otherwise("NQ"))\
        .withColumn('ci', when((JOIN_CGI.trx_lacci == JOIN_CGI.cgi_post_1), (JOIN_CGI.ci_1))\
                .when((JOIN_CGI.mtd_flag == "1"), (JOIN_CGI.mtd_ci))\
                .when((JOIN_CGI.mm_flag == "1"), (JOIN_CGI.mm_ci)).otherwise("NQ"))\
        .withColumn('node_type', when((JOIN_CGI.trx_lacci == JOIN_CGI.cgi_post_1), (JOIN_CGI.node_1))\
                .when((JOIN_CGI.mtd_flag == "1"), (JOIN_CGI.mtd_node))\
                .when((JOIN_CGI.mm_flag == "1"), (JOIN_CGI.mm_node)).otherwise("NQ"))\
        .withColumn('area_sales', when((JOIN_CGI.trx_lacci == JOIN_CGI.cgi_post_1), (JOIN_CGI.area_sales_1))\
                .when((JOIN_CGI.mtd_flag == "1"), (JOIN_CGI.mtd_area_name))\
                .when((JOIN_CGI.mm_flag == "1"), (JOIN_CGI.mm_area_name)).otherwise("NQ"))\
        .withColumn('region_sales', when((JOIN_CGI.trx_lacci == JOIN_CGI.cgi_post_1), (JOIN_CGI.region_sales_1))\
                .when((JOIN_CGI.mtd_flag == "1"), (JOIN_CGI.mtd_region_sales))\
                .when((JOIN_CGI.mm_flag == "1"), (JOIN_CGI.mm_region_sales)).otherwise("NQ"))\
        .withColumn('branch', when((JOIN_CGI.trx_lacci == JOIN_CGI.cgi_post_1), (JOIN_CGI.branch_1))\
                .when((JOIN_CGI.mtd_flag == "1"), (JOIN_CGI.mtd_branch))\
                .when((JOIN_CGI.mm_flag == "1"), (JOIN_CGI.mm_branch)).otherwise("NQ"))\
        .withColumn('subbranch', when((JOIN_CGI.trx_lacci == JOIN_CGI.cgi_post_1), (JOIN_CGI.subbranch_1))\
                .when((JOIN_CGI.mtd_flag == "1"), (JOIN_CGI.mtd_subbranch))\
                .when((JOIN_CGI.mm_flag == "1"), (JOIN_CGI.mm_subbranch)).otherwise("NQ"))\
        .withColumn('cluster_sales', when((JOIN_CGI.trx_lacci == JOIN_CGI.cgi_post_1), (JOIN_CGI.cluster_sales_1))\
                .when((JOIN_CGI.mtd_flag == "1"), (JOIN_CGI.mtd_cluster_sales))\
                .when((JOIN_CGI.mm_flag == "1"), (JOIN_CGI.mm_cluster_sales)).otherwise("NQ"))\
        .withColumn('provinsi', when((JOIN_CGI.trx_lacci == JOIN_CGI.cgi_post_1), (JOIN_CGI.provinsi_1))\
                .when((JOIN_CGI.mtd_flag == "1"), (JOIN_CGI.mtd_provinsi))\
                .when((JOIN_CGI.mm_flag == "1"), (JOIN_CGI.mm_provinsi)).otherwise("NQ"))\
        .withColumn('kabupaten', when((JOIN_CGI.trx_lacci == JOIN_CGI.cgi_post_1), (JOIN_CGI.kabupaten_1))\
                .when((JOIN_CGI.mtd_flag == "1"), (JOIN_CGI.mtd_kabupaten))\
                .when((JOIN_CGI.mm_flag == "1"), (JOIN_CGI.mm_kabupaten)).otherwise("NQ"))\
        .withColumn('kecamatan', when((JOIN_CGI.trx_lacci == JOIN_CGI.cgi_post_1), (JOIN_CGI.kecamatan_1))\
                .when((JOIN_CGI.mtd_flag == "1"), (JOIN_CGI.mtd_kecamatan))\
                .when((JOIN_CGI.mm_flag == "1"), (JOIN_CGI.mm_kecamatan)).otherwise("NQ"))\
        .withColumn('kelurahan', when((JOIN_CGI.trx_lacci == JOIN_CGI.cgi_post_1), (JOIN_CGI.kelurahan_1))\
                .when((JOIN_CGI.mtd_flag == "1"), (JOIN_CGI.mtd_kelurahan))\
                .when((JOIN_CGI.mm_flag == "1"), (JOIN_CGI.mm_kelurahan)).otherwise("NQ"))
LACIMMA2=JOIN_CGI.select('timestamp_r','trx_hour','trx_date','msisdn','subs_id','cust_type_desc','cust_subtype_desc','account','recharge_channel','expiration_date','serial_number','deltabalance','balance_amount','credit_indicator','recharge_method','recharge_id','bonus_information','providerid','sourceip','userid','resultcode','bank_code','a_number_location','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','balance_before','adjustment_reason','caseid','crmuserid','old_expiration_date','split_code','recharge_amount','future_string_1','future_string_2','future_string_3','brand','load_ts','load_user','sitename','event_date','filename','rec_id','retry_count','status_data','status','trx_lacci','flag_status')

JOIN_LACCI=STATUS_reject.join(LACIMMA,STATUS_reject.trx_lacci ==  LACIMMA.lacci_id_1, how = 'left')
JOIN_LACCI=JOIN_LACCI.withColumn('lacci_id', when((JOIN_LACCI.trx_lacci == JOIN_LACCI.lacci_id_1) & (JOIN_LACCI.area_sales_1 != "UNKNOWN"), (JOIN_LACCI.lacci_id_1))\
                .when((JOIN_LACCI.mtd_flag == "1") & (JOIN_LACCI.mtd_area_name != "UNKNOWN"), (JOIN_LACCI.mtd_lacci_id))\
                .when((JOIN_LACCI.mm_flag == "1") & (JOIN_LACCI.mm_area_name != "UNKNOWN"), (JOIN_LACCI.mm_lacci_id)).otherwise("-99"))\
        .withColumn('lacci_closing_flag', when((JOIN_LACCI.trx_lacci == JOIN_LACCI.lacci_id_1) & (JOIN_LACCI.area_sales_1 != "UNKNOWN"), ("0"))\
                .when((JOIN_LACCI.mtd_flag == "1") & (JOIN_LACCI.mtd_area_name != "UNKNOWN"), ("1"))\
                .when((JOIN_LACCI.mm_flag == "1") & (JOIN_LACCI.mm_area_name != "UNKNOWN"), ("2")).otherwise("-1") )\
        .withColumn('lac', when((JOIN_LACCI.trx_lacci == JOIN_LACCI.lacci_id_1), (JOIN_LACCI.lac_1))\
                .when((JOIN_LACCI.mtd_flag == "1"), (JOIN_LACCI.mtd_lac))\
                .when((JOIN_LACCI.mm_flag == "1"), (JOIN_LACCI.mm_lac)).otherwise("NQ"))\
        .withColumn('ci', when((JOIN_LACCI.trx_lacci == JOIN_LACCI.lacci_id_1), (JOIN_LACCI.ci_1))\
                .when((JOIN_LACCI.mtd_flag == "1"), (JOIN_LACCI.mtd_ci))\
                .when((JOIN_LACCI.mm_flag == "1"), (JOIN_LACCI.mm_ci)).otherwise("NQ"))\
        .withColumn('node_type', when((JOIN_LACCI.trx_lacci == JOIN_LACCI.lacci_id_1), (JOIN_LACCI.node_1))\
                .when((JOIN_LACCI.mtd_flag == "1"), (JOIN_LACCI.mtd_node))\
                .when((JOIN_LACCI.mm_flag == "1"), (JOIN_LACCI.mm_node)).otherwise("NQ"))\
        .withColumn('area_sales', when((JOIN_LACCI.trx_lacci == JOIN_LACCI.lacci_id_1), (JOIN_LACCI.area_sales_1))\
                .when((JOIN_LACCI.mtd_flag == "1"), (JOIN_LACCI.mtd_area_name))\
                .when((JOIN_LACCI.mm_flag == "1"), (JOIN_LACCI.mm_area_name)).otherwise("NQ"))\
        .withColumn('region_sales', when((JOIN_LACCI.trx_lacci == JOIN_LACCI.lacci_id_1), (JOIN_LACCI.region_sales_1))\
                .when((JOIN_LACCI.mtd_flag == "1"), (JOIN_LACCI.mtd_region_sales))\
                .when((JOIN_LACCI.mm_flag == "1"), (JOIN_LACCI.mm_region_sales)).otherwise("NQ"))\
        .withColumn('branch', when((JOIN_LACCI.trx_lacci == JOIN_LACCI.lacci_id_1), (JOIN_LACCI.branch_1))\
                .when((JOIN_LACCI.mtd_flag == "1"), (JOIN_LACCI.mtd_branch))\
                .when((JOIN_LACCI.mm_flag == "1"), (JOIN_LACCI.mm_branch)).otherwise("NQ"))\
        .withColumn('subbranch', when((JOIN_LACCI.trx_lacci == JOIN_LACCI.lacci_id_1), (JOIN_LACCI.subbranch_1))\
                .when((JOIN_LACCI.mtd_flag == "1"), (JOIN_LACCI.mtd_subbranch))\
                .when((JOIN_LACCI.mm_flag == "1"), (JOIN_LACCI.mm_subbranch)).otherwise("NQ"))\
        .withColumn('cluster_sales', when((JOIN_LACCI.trx_lacci == JOIN_LACCI.lacci_id_1), (JOIN_LACCI.cluster_sales_1))\
                .when((JOIN_LACCI.mtd_flag == "1"), (JOIN_LACCI.mtd_cluster_sales))\
                .when((JOIN_LACCI.mm_flag == "1"), (JOIN_LACCI.mm_cluster_sales)).otherwise("NQ"))\
        .withColumn('provinsi', when((JOIN_LACCI.trx_lacci == JOIN_LACCI.lacci_id_1), (JOIN_LACCI.provinsi_1))\
                .when((JOIN_LACCI.mtd_flag == "1"), (JOIN_LACCI.mtd_provinsi))\
                .when((JOIN_LACCI.mm_flag == "1"), (JOIN_LACCI.mm_provinsi)).otherwise("NQ"))\
        .withColumn('kabupaten', when((JOIN_LACCI.trx_lacci == JOIN_LACCI.lacci_id_1), (JOIN_LACCI.kabupaten_1))\
                .when((JOIN_LACCI.mtd_flag == "1"), (JOIN_LACCI.mtd_kabupaten))\
                .when((JOIN_LACCI.mm_flag == "1"), (JOIN_LACCI.mm_kabupaten)).otherwise("NQ"))\
        .withColumn('kecamatan', when((JOIN_LACCI.trx_lacci == JOIN_LACCI.lacci_id_1), (JOIN_LACCI.kecamatan_1))\
                .when((JOIN_LACCI.mtd_flag == "1"), (JOIN_LACCI.mtd_kecamatan))\
                .when((JOIN_LACCI.mm_flag == "1"), (JOIN_LACCI.mm_kecamatan)).otherwise("NQ"))\
        .withColumn('kelurahan', when((JOIN_LACCI.trx_lacci == JOIN_LACCI.lacci_id_1), (JOIN_LACCI.kelurahan_1))\
                .when((JOIN_LACCI.mtd_flag == "1"), (JOIN_LACCI.mtd_kelurahan))\
                .when((JOIN_LACCI.mm_flag == "1"), (JOIN_LACCI.mm_kelurahan)).otherwise("NQ"))
LACIMMA3=JOIN_LACCI.select('timestamp_r','trx_hour','trx_date','msisdn','subs_id','cust_type_desc','cust_subtype_desc','account','recharge_channel','expiration_date','serial_number','deltabalance','balance_amount','credit_indicator','recharge_method','recharge_id','bonus_information','providerid','sourceip','userid','resultcode','bank_code','a_number_location','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','balance_before','adjustment_reason','caseid','crmuserid','old_expiration_date','split_code','recharge_amount','future_string_1','future_string_2','future_string_3','brand','load_ts','load_user','sitename','event_date','filename','rec_id','retry_count','status_data','status','trx_lacci','flag_status')

from functools import reduce 
df = [ LACIMMA1,LACIMMA2,LACIMMA3]
merge = reduce(DataFrame.unionAll, df)

merge.write.csv('file:///home/hdoop/spark-3.3.1-bin-hadoop3/RCG_1',sep ='~')
RNR_REJECT.write.csv('file:///home/hdoop/spark-3.3.1-bin-hadoop3/RCG_1_REJECT',sep ='~')

####
hv.sql("use RCG_1")
hv.sql("CREATE TABLE IF NOT EXISTS RCG_PART_1 (timestamp_r string,trx_hour string,trx_date string,msisdn string,subs_id string,cust_type_desc string,cust_subtype_desc string,account string,recharge_channel string,expiration_date string,serial_number string,deltabalance string,balance_amount string,credit_indicator string,recharge_method string,recharge_id string,bonus_information string,providerid string,sourceip string,userid string,resultcode string,bank_code string,a_number_location string,lacci_id string,lacci_closing_flag string,lac string,ci string,node_type string,area_sales string,region_sales string,branch string,subbranch string,cluster_sales string,provinsi string,kabupaten string,kecamatan string,kelurahan string,indicator_4g string,balance_before string,adjustment_reason string,caseid string,crmuserid string,old_expiration_date string,split_code string,recharge_amount string,future_string_1 string,future_string_2 string,future_string_3 string,brand string,load_ts string,load_user string,sitename string,event_date string,filename string,rec_id string,retry_count string,status_data string,status string,trx_lacci string,flag_status string)\
        row format delimited fields terminated by '~'\
    ")
hv.sql("load data local inpath 'file:///home/hdoop/spark-3.3.1-bin-hadoop3/RCG_1' overwrite into table RCG_PART_1")
end_date = datetime.now() + timedelta(hours=7)
duration = (end_date - start_date).seconds
###
print(start_date.strftime('%Y-%m-%d %H:%M:%S'))
print(end_date.strftime('%Y-%m-%d %H:%M:%S'))
print(duration)
