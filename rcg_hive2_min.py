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

###
hv.sql("use RCG_1")
hv.sql("CREATE TABLE IF NOT EXISTS bonus (product_name string,price string,l1 string,l2 string,l3 string,l4 string,note string,source string)\
    row format delimited fields terminated by '|'\
    ")
hv.sql("load data local inpath 'file:///home/hdoop/PRODUCT_BONUS.txt' overwrite into table bonus")
RCG_1=hv.table('RCG_1.RCG_PART_1')
RCG_1=RCG_1.withColumn('fil', when((RCG_1.flag_status == "NODUP"), "OKE").otherwise("GAOKE"))

DUP01=RCG_1.filter(col('fil')  == 'GAOKE').select('timestamp_r','trx_hour','trx_date','msisdn','subs_id','cust_type_desc','cust_subtype_desc','account','recharge_channel','expiration_date','serial_number','deltabalance','balance_amount','credit_indicator','recharge_method','recharge_id','bonus_information','providerid','sourceip','userid','resultcode','bank_code','a_number_location','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','balance_before','adjustment_reason','caseid','crmuserid','old_expiration_date','split_code','recharge_amount','future_string_1','future_string_2','future_string_3','brand','load_ts','load_user','sitename','event_date','filename','rec_id','retry_count','status_data','status','trx_lacci','flag_status')
NODUP=RCG_1.filter(col('fil')  == 'OKE')

from pyspark.sql.window import Window
from pyspark.sql.functions import rank
windowSpec  = Window.partitionBy('filename','rec_id')\
    .orderBy('timestamp_r','trx_hour','trx_date','msisdn','subs_id','cust_type_desc','cust_subtype_desc','account','recharge_channel','expiration_date','serial_number','deltabalance','balance_amount','credit_indicator','recharge_method','recharge_id','bonus_information','providerid','sourceip','userid','resultcode','bank_code','a_number_location','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','balance_before','adjustment_reason','caseid','crmuserid','old_expiration_date','split_code','recharge_amount','future_string_1','future_string_2','future_string_3','brand','load_ts','load_user','sitename','event_date','filename','rec_id','retry_count','status_data','status','trx_lacci','flag_status')
NODUP=NODUP.withColumn("RDUP03",rank().over(windowSpec))\
    .withColumn('status', when((F.col('RDUP03') > 1), "DUP").otherwise(F.col('status')))\
    .withColumn('filter2', when((F.col('RDUP03') == 1), "OKE").otherwise("GAOKE"))

DUP03=NODUP.filter(col('filter2')  == 'GAOKE').select('timestamp_r','trx_hour','trx_date','msisdn','subs_id','cust_type_desc','cust_subtype_desc','account','recharge_channel','expiration_date','serial_number','deltabalance','balance_amount','credit_indicator','recharge_method','recharge_id','bonus_information','providerid','sourceip','userid','resultcode','bank_code','a_number_location','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','balance_before','adjustment_reason','caseid','crmuserid','old_expiration_date','split_code','recharge_amount','future_string_1','future_string_2','future_string_3','brand','load_ts','load_user','sitename','event_date','filename','rec_id','retry_count','status_data','status','trx_lacci','flag_status')
NODUP03=NODUP.filter(col('filter2')  == 'OKE').select('timestamp_r','trx_hour','trx_date','msisdn','subs_id','cust_type_desc','cust_subtype_desc','account','recharge_channel','expiration_date','serial_number','deltabalance','balance_amount','credit_indicator','recharge_method','recharge_id','bonus_information','providerid','sourceip','userid','resultcode','bank_code','a_number_location','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','balance_before','adjustment_reason','caseid','crmuserid','old_expiration_date','split_code','recharge_amount','future_string_1','future_string_2','future_string_3','brand','load_ts','load_user','sitename','event_date','filename','rec_id','retry_count','status_data','status','trx_lacci','flag_status','RDUP03')

windowSpec  = Window.partitionBy('timestamp_r','msisdn','account','deltabalance')\
    .orderBy('event_date','filename','rec_id')
NODUP=NODUP03.withColumn("RDUP02",rank().over(windowSpec))\
    .withColumn('status', when((F.col('RDUP02') > 1), "DUP").otherwise(F.col('status')))\
    .withColumn('filter2', when((F.col('RDUP02') == 1), "OKE").otherwise("GAOKE"))
            
DUP02=NODUP.filter(col('filter2')  == 'GAOKE').select('timestamp_r','trx_hour','trx_date','msisdn','subs_id','cust_type_desc','cust_subtype_desc','account','recharge_channel','expiration_date','serial_number','deltabalance','balance_amount','credit_indicator','recharge_method','recharge_id','bonus_information','providerid','sourceip','userid','resultcode','bank_code','a_number_location','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','balance_before','adjustment_reason','caseid','crmuserid','old_expiration_date','split_code','recharge_amount','future_string_1','future_string_2','future_string_3','brand','load_ts','load_user','sitename','event_date','filename','rec_id','retry_count','status_data','status','trx_lacci','flag_status')
NODUP02=NODUP.filter(col('filter2')  == 'OKE').select('timestamp_r','trx_hour','trx_date','msisdn','subs_id','cust_type_desc','cust_subtype_desc','account','recharge_channel','expiration_date','serial_number','deltabalance','balance_amount','credit_indicator','recharge_method','recharge_id','bonus_information','providerid','sourceip','userid','resultcode','bank_code','a_number_location','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','balance_before','adjustment_reason','caseid','crmuserid','old_expiration_date','split_code','recharge_amount','future_string_1','future_string_2','future_string_3','brand','load_ts','load_user','sitename','event_date','filename','rec_id','retry_count','status_data','status','trx_lacci','flag_status','RDUP03','RDUP02')


NODUP02=NODUP02.withColumn('status_data', when((NODUP02.subs_id == "-99") | (NODUP02.cust_type_desc == "NQ") | (NODUP02.cust_subtype_desc == "NQ") | (NODUP02.lacci_id == "-99") | (NODUP02.lacci_closing_flag == "-1") | (NODUP02.lac == "NQ") | (NODUP02.ci == "NQ") | (NODUP02.node_type == "NQ") | (NODUP02.area_sales == "NQ") | (NODUP02.region_sales == "NQ") | (NODUP02.branch == "NQ") | (NODUP02.subbranch == "NQ") | (NODUP02.cluster_sales == "NQ") | (NODUP02.provinsi == "NQ") | (NODUP02.kabupaten == "NQ") | (NODUP02.kecamatan == "NQ") | (NODUP02.kelurahan == "NQ"), "REJECT").otherwise(("GOOD")))\
                .withColumn('period' ,regexp_replace("timestamp_r","-",""))\
                .withColumn('qty', lit(1))\
                .withColumn('group_process', lit(""))\
                .withColumn('job_id', lit(""))\
                .withColumn('rev', lit(NODUP02.recharge_amount)).fillna("0",["rev"])\
                .withColumn('retry_ts', lit(NODUP02.trx_date))\
                .withColumn('load_user', lit('Solusi247'))

RCG_SUM= NODUP02.groupBy('event_date','filename','job_id','group_process','status_data','status').agg(sum('qty').alias('qty'),sum('rev').alias('rev'))\
                .select('event_date','filename','status_data','status','job_id','group_process','qty','rev')\
                .withColumn('period' ,regexp_replace("event_date","-",""))\
                .withColumn('job_id', lit("23022023"))\
                .withColumn('group_process', lit(2))\
                .withColumn('filetype', lit('SA_USAGE_OCS_RCG'))\
                .select('period','filename','job_id','group_process','status_data','qty','rev','filetype','status')

SA_RCG=NODUP02.withColumn('fil', when((NODUP02.status_data != "REJECT"), "OKE").otherwise('GAOKE'))

RCG_REJECT=SA_RCG.filter(col('fil')  == 'GAOKE').select('timestamp_r','trx_hour','trx_date','msisdn','subs_id','cust_type_desc','cust_subtype_desc','account','recharge_channel','expiration_date','serial_number','deltabalance','balance_amount','credit_indicator','recharge_method','recharge_id','bonus_information','providerid','sourceip','userid','resultcode','bank_code','a_number_location','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','balance_before','adjustment_reason','caseid','crmuserid','old_expiration_date','split_code','recharge_amount','future_string_1','future_string_2','future_string_3','brand','load_ts','load_user','sitename','event_date','filename','rec_id','retry_count','retry_ts')
SA_RCG=SA_RCG.filter(col('fil')  == 'OKE').select('timestamp_r','trx_hour','trx_date','msisdn','subs_id','cust_type_desc','cust_subtype_desc','account','recharge_channel','expiration_date','serial_number','deltabalance','balance_amount','credit_indicator','recharge_method','recharge_id','bonus_information','providerid','sourceip','userid','resultcode','bank_code','a_number_location','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','balance_before','adjustment_reason','caseid','crmuserid','old_expiration_date','split_code','recharge_amount','future_string_1','future_string_2','future_string_3','brand','load_ts','load_user','sitename','event_date','filename','rec_id','retry_count','retry_ts')

MERGE_RCG=NODUP02.withColumn('recharge', upper(NODUP02.recharge_method))\
                .withColumn('offer_id', when ((NODUP02.status_data == "GOOD"), (NODUP02.providerid)).otherwise("")).withColumn('subs_offer', substring('offer_id',3,5))\
                .filter(col('recharge') == 'TPFEE')\
                .withColumn('mm_closing_flag', lit(1))\
                .withColumn('load_date', lit(20230222))\
                .withColumn('l1_name', lit('Digital Services'))\
                .withColumn('l2_name', lit('Transfer Pulsa'))\
                .withColumn('l3_name', lit('Simpati'))\
                .withColumn('l4_name', lit('SA_50007'))\
                .withColumn('offer_name', lit('CONSUMER'))\
                .withColumn('subtype', lit(''))\
                .withColumn('payment_type', lit(''))
      
BONUS=hv.table('RCG_1.bonus')\
            .withColumn('source', upper(F.col('source')))\
            .filter(col('source') == 'PAYD')\
            .select('product_name','price','l1','l2','l3','l4','note','source')
            
CASE_BONUS=BONUS.withColumn('L1', when((BONUS.l1 == "UNKNOWN"), "others").when((BONUS.l1 == "Digital Service"), "Digital ServiceS").otherwise(BONUS.l1))\
            .withColumn('L2', when((BONUS.l2 == "UNKNOWN"), "others").when((BONUS.l2 == "Other VAS Services"), "Other VAS Service")\
                .when((BONUS.l2 == "SMS Non P to P"), "SMS Non P2P").otherwise(BONUS.l2))\
            .withColumn('l3', when((F.col('L1') == "Voice P2P") & (F.col('L2') == "Internal MO Call") & (F.col('L3') == "Local"), ("Local (On-Net)")).otherwise(F.col('l3')))\
            .withColumn('l4', when((F.col('L1') == "UNKNOWN") | (F.col('L2') == "Others"), "Others").otherwise(F.col('L4')))\
            .withColumn('trim_note', trim(F.col('note')))


JOIN_BONUS=MERGE_RCG.join(CASE_BONUS,MERGE_RCG.subs_offer ==  CASE_BONUS.trim_note, how = 'left')
#JOIN_BONUS=JOIN_BONUS.select('subs_offer','trim_note')
JOIN_BONUS=JOIN_BONUS.withColumn('lacci_id', when((JOIN_BONUS.region_sales == 'UNKNOWN'), 'UNKNOWN').otherwise(JOIN_BONUS.lacci_id))\
            .withColumn('node_type', when((JOIN_BONUS.region_sales == 'UNKNOWN'), 'UNKNOWN').otherwise(JOIN_BONUS.node_type))\
            .withColumn('area_sales', when((JOIN_BONUS.region_sales == 'UNKNOWN'), 'UNKNOWN').otherwise(JOIN_BONUS.area_sales))\
            .withColumn('region_sales', when((JOIN_BONUS.region_sales == 'UNKNOWN'), 'UNKNOWN').otherwise(JOIN_BONUS.region_sales))
JOIN_BONUS=JOIN_BONUS.withColumn('branch', when((JOIN_BONUS.region_sales == 'UNKNOWN'), 'UNKNOWN').otherwise(JOIN_BONUS.branch))\
			.withColumn('subbranch', when((JOIN_BONUS.region_sales == 'UNKNOWN'), 'UNKNOWN').otherwise(JOIN_BONUS.subbranch))\
			.withColumn('cluster_sales', when((JOIN_BONUS.region_sales == 'UNKNOWN'), 'UNKNOWN').otherwise(JOIN_BONUS.cluster_sales))\
			.withColumn('provinsi', when((JOIN_BONUS.region_sales == 'UNKNOWN'), 'UNKNOWN').otherwise(JOIN_BONUS.provinsi))\
            .withColumn('kabupaten', when((JOIN_BONUS.region_sales == 'UNKNOWN'), 'UNKNOWN').otherwise(JOIN_BONUS.kabupaten))\
            .withColumn('kecamatan', when((JOIN_BONUS.region_sales == 'UNKNOWN'), 'UNKNOWN').otherwise(JOIN_BONUS.kecamatan))\
            .withColumn('kelurahan', when((JOIN_BONUS.region_sales == 'UNKNOWN'), 'UNKNOWN').otherwise(JOIN_BONUS.kelurahan))\
            .withColumn('mm_closing_flag', lit(1))\
            .withColumn('l1_name', when((JOIN_BONUS.subs_offer == JOIN_BONUS.trim_note), (JOIN_BONUS.l1_name)).otherwise("Others"))\
            .withColumn('l2_name', when((JOIN_BONUS.subs_offer == JOIN_BONUS.trim_note), (JOIN_BONUS.l2_name)).otherwise("Others"))\
            .withColumn('l3_name', when((JOIN_BONUS.subs_offer == JOIN_BONUS.trim_note), (JOIN_BONUS.l3_name)).otherwise("Others"))\
            .withColumn('l4_name', when((JOIN_BONUS.subs_offer == JOIN_BONUS.trim_note), (JOIN_BONUS.l4_name)).otherwise("Others"))\
            .withColumn('brand', lit("ByU"))\
            .withColumn('cust_type_desc', lit("B2C"))\
            .withColumn('cust_subtype_desc', lit("NON HVC"))\
            .withColumn('service_filter', lit(""))\
            .withColumn('payment_channel', lit(""))\
            .withColumn('trx', lit(1))\
            .withColumn('dur', lit(0))\
            .withColumn('vol', lit(0))\
            .withColumn('source_name', when((JOIN_BONUS.status_data == "GOOD"), "RCG_GOOD").otherwise("RCG_REJECT"))\
            .withColumn('source', when((JOIN_BONUS.status_data == "GOOD"), "RCG_GOOD").otherwise("RCG_REJECT"))\
            .withColumn('event_date', when((JOIN_BONUS.status_data == "GOOD"), (JOIN_BONUS.trx_date)).otherwise(JOIN_BONUS.event_date))
            
RCG=JOIN_BONUS.withColumn('filter_source', when((JOIN_BONUS.source == "RCG_GOOD"), "OKE").otherwise("GAOKE"))\
                .fillna("0",["trx"])\
                .fillna("0",["rev"])\
                .fillna("0",["dur"])\
                .fillna("0",["vol"])
RCG_MERGE_GOOD=RCG.filter(col('filter_source') == 'OKE').select('trx_date','msisdn','subs_id','lacci_id','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','lacci_closing_flag','mm_closing_flag','l1_name','l2_name','l3_name','l4_name','offer_id','offer_name','brand','cust_type_desc','cust_subtype_desc','service_filter','payment_channel','trx','rev','dur','vol','source_name','load_ts','source','job_id','event_date','load_date')
RCG_MERGE_REJECT=RCG.filter(col('filter_source') == 'GAOKE').select('trx_date','msisdn','subs_id','lacci_id','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','lacci_closing_flag','mm_closing_flag','l1_name','l2_name','l3_name','l4_name','offer_id','offer_name','brand','cust_type_desc','cust_subtype_desc','service_filter','payment_channel','trx','rev','dur','vol','source_name','load_ts','source','job_id','event_date','load_date')
#CEK=MERGE_RCG.filter(col('recharge') == 'TPFEE')

RCG_MERGE_GOOD=RCG_MERGE_GOOD.groupBy('trx_date','msisdn','subs_id','lacci_id','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','lacci_closing_flag','mm_closing_flag','l1_name','l2_name','l3_name','l4_name','offer_id','offer_name','brand','cust_type_desc','cust_subtype_desc','service_filter','payment_channel','source_name','load_ts','source','job_id','event_date','load_date')\
    .agg(sum('trx').alias('trx')\
    ,sum('rev').alias('rev')\
    ,sum('dur').alias('dur')\
    ,sum('vol').alias('vol'))
RCG_MERGE_GOOD=RCG_MERGE_GOOD.withColumn('trx', lit(RCG_MERGE_GOOD.trx).cast(IntegerType()))\
            .withColumn('dur',lit(RCG_MERGE_GOOD.dur).cast(IntegerType()))\
            .withColumn('rev', lit(RCG_MERGE_GOOD.rev).cast(IntegerType()))\
            .withColumn('vol', lit(RCG_MERGE_GOOD.vol).cast(IntegerType()))\
            .select('trx_date','msisdn','subs_id','lacci_id','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','lacci_closing_flag','mm_closing_flag','l1_name','l2_name','l3_name','l4_name','offer_id','offer_name','brand','cust_type_desc','cust_subtype_desc','service_filter','payment_channel','trx','rev','dur','vol','source_name','load_ts','source','job_id','event_date','load_date')

RCG_MERGE_REJECT=RCG_MERGE_REJECT.groupBy('trx_date','msisdn','subs_id','lacci_id','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','lacci_closing_flag','mm_closing_flag','l1_name','l2_name','l3_name','l4_name','offer_id','offer_name','brand','cust_type_desc','cust_subtype_desc','service_filter','payment_channel','source_name','load_ts','source','job_id','event_date','load_date')\
    .agg(sum('trx').alias('trx')\
    ,sum('rev').alias('rev')\
    ,sum('dur').alias('dur')\
    ,sum('vol').alias('vol'))
RCG_MERGE_REJECT=RCG_MERGE_REJECT.withColumn('trx', lit(RCG_MERGE_REJECT.trx).cast(IntegerType()))\
            .withColumn('dur',lit(RCG_MERGE_REJECT.dur).cast(IntegerType()))\
            .withColumn('rev', lit(RCG_MERGE_REJECT.rev).cast(IntegerType()))\
            .withColumn('vol', lit(RCG_MERGE_REJECT.vol).cast(IntegerType()))\
            .select('trx_date','msisdn','subs_id','lacci_id','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','indicator_4g','lacci_closing_flag','mm_closing_flag','l1_name','l2_name','l3_name','l4_name','offer_id','offer_name','brand','cust_type_desc','cust_subtype_desc','service_filter','payment_channel','trx','rev','dur','vol','source_name','load_ts','source','job_id','event_date','load_date')

DUP03.write.csv('file:///home/hdoop/spark-3.3.1-bin-hadoop3/RCG_DUP3',sep ='~')
DUP02.write.csv('file:///home/hdoop/spark-3.3.1-bin-hadoop3/RCG_DUP2',sep ='~')
DUP01.write.csv('file:///home/hdoop/spark-3.3.1-bin-hadoop3/RCG_DUP1',sep ='~')
RCG_REJECT.write.csv('file:///home/hdoop/spark-3.3.1-bin-hadoop3/SA_RCG_REJECT',sep ='~')
SA_RCG.write.csv('file:///home/hdoop/spark-3.3.1-bin-hadoop3/SA_RCG',sep ='~')
RCG_SUM.write.csv('file:///home/hdoop/spark-3.3.1-bin-hadoop3/RCG_SUM',sep ='~')
RCG_MERGE_GOOD.write.csv('file:///home/hdoop/spark-3.3.1-bin-hadoop3/RCG_MERGE_GOOD',sep ='~')
RCG_MERGE_REJECT.write.csv('file:///home/hdoop/spark-3.3.1-bin-hadoop3/RCG_MERGE_REJECT',sep ='~')


####
hv.sql("use RCG_1")
hv.sql("CREATE TABLE IF NOT EXISTS RCG_DUP3 (timestamp_r string,trx_hour string,trx_date string,msisdn string,subs_id string,cust_type_desc string,cust_subtype_desc string,account string,recharge_channel string,expiration_date string,serial_number string,deltabalance string,balance_amount string,credit_indicator string,recharge_method string,recharge_id string,bonus_information string,providerid string,sourceip string,userid string,resultcode string,bank_code string,a_number_location string,lacci_id string,lacci_closing_flag string,lac string,ci string,node_type string,area_sales string,region_sales string,branch string,subbranch string,cluster_sales string,provinsi string,kabupaten string,kecamatan string,kelurahan string,indicator_4g string,balance_before string,adjustment_reason string,caseid string,crmuserid string,old_expiration_date string,split_code string,recharge_amount string,future_string_1 string,future_string_2 string,future_string_3 string,brand string,load_ts string,load_user string,sitename string,event_date string,filename string,rec_id string,retry_count string,status_data string,status string,trx_lacci string,flag_status string)\
    row format delimited fields terminated by '~'\
    ")
hv.sql("load data local inpath 'file:///home/hdoop/spark-3.3.1-bin-hadoop3/RCG_DUP3' overwrite into table RCG_DUP3")

hv.sql("CREATE TABLE IF NOT EXISTS RCG_DUP2 (timestamp_r string,trx_hour string,trx_date string,msisdn string,subs_id string,cust_type_desc string,cust_subtype_desc string,account string,recharge_channel string,expiration_date string,serial_number string,deltabalance string,balance_amount string,credit_indicator string,recharge_method string,recharge_id string,bonus_information string,providerid string,sourceip string,userid string,resultcode string,bank_code string,a_number_location string,lacci_id string,lacci_closing_flag string,lac string,ci string,node_type string,area_sales string,region_sales string,branch string,subbranch string,cluster_sales string,provinsi string,kabupaten string,kecamatan string,kelurahan string,indicator_4g string,balance_before string,adjustment_reason string,caseid string,crmuserid string,old_expiration_date string,split_code string,recharge_amount string,future_string_1 string,future_string_2 string,future_string_3 string,brand string,load_ts string,load_user string,sitename string,event_date string,filename string,rec_id string,retry_count string,status_data string,status string,trx_lacci string,flag_status string)\
    row format delimited fields terminated by '~'\
    ")
hv.sql("load data local inpath 'file:///home/hdoop/spark-3.3.1-bin-hadoop3/RCG_DUP2' overwrite into table RCG_DUP2")

hv.sql("CREATE TABLE IF NOT EXISTS RCG_DUP1 (timestamp_r string,trx_hour string,trx_date string,msisdn string,subs_id string,cust_type_desc string,cust_subtype_desc string,account string,recharge_channel string,expiration_date string,serial_number string,deltabalance string,balance_amount string,credit_indicator string,recharge_method string,recharge_id string,bonus_information string,providerid string,sourceip string,userid string,resultcode string,bank_code string,a_number_location string,lacci_id string,lacci_closing_flag string,lac string,ci string,node_type string,area_sales string,region_sales string,branch string,subbranch string,cluster_sales string,provinsi string,kabupaten string,kecamatan string,kelurahan string,indicator_4g string,balance_before string,adjustment_reason string,caseid string,crmuserid string,old_expiration_date string,split_code string,recharge_amount string,future_string_1 string,future_string_2 string,future_string_3 string,brand string,load_ts string,load_user string,sitename string,event_date string,filename string,rec_id string,retry_count string,status_data string,status string,trx_lacci string,flag_status string)\
    row format delimited fields terminated by '~'\
    ")
hv.sql("load data local inpath 'file:///home/hdoop/spark-3.3.1-bin-hadoop3/RCG_DUP1' overwrite into table RCG_DUP1")

hv.sql("CREATE TABLE IF NOT EXISTS SA_RCG_REJECT (timestamp_r string,trx_hour string,trx_date string,msisdn string,subs_id string,cust_type_desc string,cust_subtype_desc string,account string,recharge_channel string,expiration_date string,serial_number string,deltabalance string,balance_amount string,credit_indicator string,recharge_method string,recharge_id string,bonus_information string,providerid string,sourceip string,userid string,resultcode string,bank_code string,a_number_location string,lacci_id string,lacci_closing_flag string,lac string,ci string,node_type string,area_sales string,region_sales string,branch string,subbranch string,cluster_sales string,provinsi string,kabupaten string,kecamatan string,kelurahan string,indicator_4g string,balance_before string,adjustment_reason string,caseid string,crmuserid string,old_expiration_date string,split_code string,recharge_amount string,future_string_1 string,future_string_2 string,future_string_3 string,brand string,load_ts string,load_user string,sitename string,event_date string,filename string,rec_id string,retry_count string,retry_ts string)\
    row format delimited fields terminated by '~'\
    ")
hv.sql("load data local inpath 'file:///home/hdoop/spark-3.3.1-bin-hadoop3/SA_RCG_REJECT' overwrite into table SA_RCG_REJECT")

hv.sql("CREATE TABLE IF NOT EXISTS SA_RCG (timestamp_r string,trx_hour string,trx_date string,msisdn string,subs_id string,cust_type_desc string,cust_subtype_desc string,account string,recharge_channel string,expiration_date string,serial_number string,deltabalance string,balance_amount string,credit_indicator string,recharge_method string,recharge_id string,bonus_information string,providerid string,sourceip string,userid string,resultcode string,bank_code string,a_number_location string,lacci_id string,lacci_closing_flag string,lac string,ci string,node_type string,area_sales string,region_sales string,branch string,subbranch string,cluster_sales string,provinsi string,kabupaten string,kecamatan string,kelurahan string,indicator_4g string,balance_before string,adjustment_reason string,caseid string,crmuserid string,old_expiration_date string,split_code string,recharge_amount string,future_string_1 string,future_string_2 string,future_string_3 string,brand string,load_ts string,load_user string,sitename string,event_date string,filename string,rec_id string,retry_count string,retry_ts string)\
    row format delimited fields terminated by '~'\
    ")
hv.sql("load data local inpath 'file:///home/hdoop/spark-3.3.1-bin-hadoop3/SA_RCG' overwrite into table SA_RCG")

hv.sql("CREATE TABLE IF NOT EXISTS RCG_SUM (period string,filename string,job_id string,group_process string,status_data string,qty string,rev string,filetype string,status string)\
    row format delimited fields terminated by '~'\
    ")
hv.sql("load data local inpath 'file:///home/hdoop/spark-3.3.1-bin-hadoop3/RCG_SUM' overwrite into table RCG_SUM")

hv.sql("CREATE TABLE IF NOT EXISTS RCG_MERGE_GOOD (trx_date string,msisdn string,subs_id string,lacci_id string,node_type string,area_sales string,region_sales string,branch string,subbranch string,cluster_sales string,provinsi string,kabupaten string,kecamatan string,kelurahan string,indicator_4g string,lacci_closing_flag string,mm_closing_flag string,l1_name string,l2_name string,l3_name string,l4_name string,offer_id string,offer_name string,brand string,cust_type_desc string,cust_subtype_desc string,service_filter string,payment_channel string,trx string,rev string,dur string,vol string,source_name string,load_ts string,source string,job_id string,event_date string,load_date string)\
    row format delimited fields terminated by '~'\
    ")
hv.sql("load data local inpath 'file:///home/hdoop/spark-3.3.1-bin-hadoop3/RCG_MERGE_GOOD' overwrite into table RCG_MERGE_GOOD")

hv.sql("CREATE TABLE IF NOT EXISTS RCG_MERGE_REJECT (trx_date string,msisdn string,subs_id string,lacci_id string,node_type string,area_sales string,region_sales string,branch string,subbranch string,cluster_sales string,provinsi string,kabupaten string,kecamatan string,kelurahan string,indicator_4g string,lacci_closing_flag string,mm_closing_flag string,l1_name string,l2_name string,l3_name string,l4_name string,offer_id string,offer_name string,brand string,cust_type_desc string,cust_subtype_desc string,service_filter string,payment_channel string,trx string,rev string,dur string,vol string,source_name string,load_ts string,source string,job_id string,event_date string,load_date string)\
    row format delimited fields terminated by '~'\
    ")
hv.sql("load data local inpath 'file:///home/hdoop/spark-3.3.1-bin-hadoop3/RCG_MERGE_REJECT' overwrite into table RCG_MERGE_REJECT")
end_date = datetime.now() + timedelta(hours=7)
duration = (end_date - start_date).seconds
###
print(start_date.strftime('%Y-%m-%d %H:%M:%S'))
print(end_date.strftime('%Y-%m-%d %H:%M:%S'))
print(duration)