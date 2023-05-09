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
hv.sql("drop database CHG_1 CASCADE")
hv.sql("create database IF NOT EXISTS CHG_1")
hv.sql("use CHG_1")
hv.sql("CREATE TABLE IF NOT EXISTS source_chg (timestamp string,a_party string,locationnumbertypea string,locationnumbera string,b_party string,locationnumbertypeb string,locationnumberb string,nrofreroutings string,imsi string,nrofnetworkreroutings string,redirectingpartyid string,precallduration string,callduration string,chargingid string,roamingflag string,vlr_number string,cell_id string,negotiatedqos string,requestedqos string,subscribedqos string,dialled_apn string,eventtype string,providerid string,currency string,subscriber_status string,forwarding_flag string,first_call_flag string,camel_roaming string,time_zone string,account_balance string,account_delta string,account_number string,account_owner string,event_source string,guiding_resource_type string,rounded_duration string,total_volume string,rounded_total_volume string,event_start_period string,charge_including_free_allowance string,discount_amount string,free_total_volume string,free_duration string,call_direction string,charge_code_description string,special_number_ind string,bonus_information string,internal_cause string,basic_cause string,time_band string,call_type string,bonus_consumed string,vas_code string,service_filter string,national_calling_zone string,national_called_zone string,international_calling_zone string,international_called_zone string,credit_indicator string,event_id string,access_code string,country_name string,cp_name string,content_id string,rating_group string,bft_indicator string,unapplied_amount string,partition_id string,rateddatetime string,area string,cost_band string,rating_offer_id string,settlement_indicator string,tap_code string,mcc_mnc string,rating_pricing_item_id string,file_identifier string,record_number string,future_string1 string,future_string2 string,future_string3 string,allowance_consumed_revenue string,ifrs_ind_for_allowance_revenue string,ifrs_ind_for_vas_event string,itemized_bill_fee string,consumed_monetary_allowance string,partial_charge_ind string)\
    row format delimited fields terminated by '|'\
    ")
hv.sql("load data local inpath 'file:///home/hdoop/spark-3.3.1-bin-hadoop3/CHG/*' overwrite into table source_chg")
hv.sql("CREATE TABLE IF NOT EXISTS subs_dim (trx_date string,subs_id string,msisdn string,account_id string,status string,pre_post_flag string,activation_date string,deactivation_date string,los string,price_plan_id string,prefix string,area_hlr string,region_hlr string,city_hlr string,cust_type_desc string,cust_subtype_desc string,customer_sub_segment string,load_ts string,load_user string,job_id string,migration_date string)\
    STORED AS PARQUET;\
    ")
hv.sql("load data local inpath 'file:///home/hdoop/CHG_REGULER/SUBS' overwrite into table subs_dim")
hv.sql("CREATE TABLE IF NOT EXISTS mtd (trx_date string,subs_id string,lacci_id string,msisdn string,cust_type_desc string,cust_subtype_desc string,area_sales string,region_sales string,branch string,subbranch string,cluster_sales string,provinsi string,kabupaten string,kecamatan string,kelurahan string,node_type string,trx string,dur string,load_ts string,load_user string,event_date string,job_id string,lacci_area_name string,lacci_region_network string,lacci_cluster_sales string,lacci_lac string,lacci_ci string,lacci_node string,lacci_region_sales string,lacci_branch string,lacci_subbranch string,lacci_provinsi string,lacci_kabupaten string,lacci_kecamatan string,lacci_kelurahan string)\
    STORED AS PARQUET;\
    ")
hv.sql("load data local inpath 'file:///home/hdoop/CHG_REGULER/MTD/*/*' overwrite into table mtd")
hv.sql("CREATE TABLE IF NOT EXISTS mm (mm_date string,subs_id string,lacci_id string,msisdn string,cust_type_desc string,cust_subtype_desc string,area_sales string,region_sales string,branch string,subbranch string,cluster_sales string,provinsi string,kabupaten string,kecamatan string,kelurahan string,node_type string,trx string,dur string,load_ts string,load_user string,event_date string,job_id string,lacci_area_name string,lacci_region_network string,lacci_cluster_sales string,lacci_lac string,lacci_ci string,lacci_node string,lacci_region_sales string,lacci_branch string,lacci_subbranch string,lacci_provinsi string,lacci_kabupaten string,lacci_kecamatan string,lacci_kelurahan string)\
    STORED AS PARQUET;\
    ")
hv.sql("load data local inpath 'file:///home/hdoop/CHG_REGULER/MTD/*/*' overwrite into table mm")
hv.sql("CREATE TABLE IF NOT EXISTS lacimma (lac_1 string,ci_1 string,cell_name_1 string,vendor_1 string,site_id_1 string,ne_id_1 string,site_name_1 string,msc_name_1 string,node_1 string,longitude_1 string,latitude_1 string,region_network_1 string,provinsi_1 string,kabupaten_1 string,kecamatan_1 string,kelurahan_1 string,region_sales_1 string,branch_1 string,subbranch_1 string,address_1 string,cluster_sales_1 string,mitra_ad_1 string,cgi_pre_1 string,cgi_post_1 string,ocs_cluster_1 string,ocs_zone_1 string,area_sales_1 string,eci_1 string,lacci_id_1 string,file_date_1 string,file_id_1 string,load_ts_1 string,load_user_1 string,event_date_1 string)\
       row format delimited fields terminated by ';'\
    ")
hv.sql("load data local inpath 'file:///home/hdoop/CHG_REGULER/LACCIMA_DIM.csv' overwrite into table lacimma")

##############
chg=hv.table('chg.source_chg')
chg=chg.withColumn("trim_call", trim(chg.callduration))\
        .withColumn("trim_charg", trim(chg.chargingid))\
        .withColumn("trim_timestamp", trim(chg.timestamp))\
        .withColumn("trim_aparty", trim(chg.a_party)).withColumn("trim_account_delta", trim(chg.account_delta)).withColumn("trim_account_balance", trim(chg.account_balance))\
        .withColumn("trim_rounded", trim(chg.rounded_duration)).withColumn("trim_rounded_total", trim(chg.rounded_total_volume)).withColumn("trim_charge_include", trim(chg.charge_including_free_allowance)).withColumn("trim_unnaple", trim(chg.unapplied_amount))\
        .withColumn('subscell1', substring('cell_id', 11,7))\
        .withColumn('cell1',F.regexp_replace('subscell1', r'^0+', ''))\
        .withColumn('subscell2', substring('cell_id', 6,5))\
        .withColumn('cell2',F.regexp_replace('subscell2', r'^0+', ''))\
        .withColumn('lenght_cell1', lit(length(chg.cell_id) - (19)).cast(StringType()))\
        .withColumn('lenght_cell2', lit(length(chg.cell_id) - (12)).cast(StringType()))\
        .withColumn('cell3',F.expr("substring(cell_id,18,lenght_cell1)"))\
        .withColumn('cell4',F.expr("substring(cell_id,11,lenght_cell2)"))\
        .withColumn('concat_1', 
                    F.concat(F.col('cell1'),F.lit('_'), F.col('cell3')))\
        .withColumn('concat_2', 
                    F.concat(F.col('cell2'),F.lit('_'), F.col('cell4')))\
        .withColumn('trim_cell', trim(chg.cell_id))\
        .withColumn('trim_cell1', substring('cell_id', 11,7)).withColumn('trim_cell2', substring('cell_id', 6,5))\
        .withColumn('trim_cell3', substring('cell_id', 18,3)).withColumn('trim_cell4', substring('cell_id', 11,5))\
        .withColumn('trx_1', 
                    F.concat(F.col('trim_cell1'),F.lit('|'), F.col('trim_cell3')))\
        .withColumn('trx_2', 
                    F.concat(F.col('trim_cell2'),F.lit('|'), F.col('trim_cell4')))\
        .withColumn('trx_1',F.regexp_replace('trx_1', r'^0+', ''))\
        .withColumn('trx_2',F.regexp_replace('trx_2', r'^0+', ''))\
        .withColumn('sitename', lit("REGULER/CHG"))
chg=chg.withColumn('status',when((chg.trim_timestamp.isNotNull()) & (chg.trim_aparty.isNotNull()) & (chg.trim_account_delta.isNotNull()), ("YES")).otherwise ("NO"))
good= chg.filter(col('status') == "YES")
chg_reject=chg.filter(col('status') == "NO")

proses1=good.withColumn("timestamp_r", F.to_timestamp("timestamp",'yyyyMMddHHmmss')).fillna("0",["account_delta"])\
        .withColumn('rundate', lit("20230120")).withColumn("trx_date", F.to_date("timestamp", "yyyyMMddHHmmss"))\
        .withColumn("trx_hour", F.hour("timestamp_r"))\
        .withColumn("b_party", when(good.eventtype == "GPRS", "0").when((good.b_party.isNull()) | (good.b_party == ("")), "0").otherwise(good.b_party))\
        .withColumn("number_of_reroutings",lit("0"))\
        .withColumn("imsi", when(good.imsi.isNull(), "0").otherwise (good.imsi))\
        .withColumn("call_duration", when((good.callduration.isNull()) | (good.trim_call == ("")), "0").otherwise(good.callduration))\
        .withColumn("charging_id", when((good.chargingid.isNull()) | (good.trim_charg == ""), "0").otherwise(good.chargingid))\
        .withColumn("cifa", when((good.charge_including_free_allowance.isNull()) | (good.trim_charge_include == ""), "0").otherwise(good.charge_including_free_allowance))\
        .withColumn("roaming_flag", when(good.roamingflag == "Y", "1").otherwise("0"))\
        .withColumn("cell_id", when((good.future_string3 == "129"), (good.concat_1)).when((good.future_string3 == "130"), (good.concat_1)).otherwise(good.concat_2))\
        .withColumn("account_balance", when((good.account_balance.isNull()) | (good.trim_account_balance == ""), "0").otherwise(good.account_balance))\
        .withColumn("rounded_duration", when((good.rounded_duration.isNull()) | (good.trim_rounded == ""), "0").otherwise(good.rounded_duration))\
        .withColumn('upper_credit_indicator', upper(F.col('credit_indicator')))\
        .withColumn("unapplied_amount", when((F.col('unapplied_amount').isNull()) | (F.col('unapplied_amount') == ""), "0").when(col('upper_credit_indicator') == "C", (col('account_delta') * -1)).otherwise(F.col('unapplied_amount')))\
        .withColumn("rev", when((F.col('account_delta').isNull()) | (F.col('account_delta') == ""), "0").when(col('upper_credit_indicator') == "C", (col('account_delta') * -1)).otherwise(F.col('account_delta')))\
        .withColumn("brand", lit('ByU'))\
        .withColumn("mcc", substring("mcc_mnc", 1,3))\
        .withColumn("network_activity_id", lit(concat(col('timestamp_r'), lit("-"), col('event_id'))))\
        .withColumn("pre_post_flag", lit("1"))\
        .withColumn("discount_amount", lit("0"))\
        .withColumn("retry_count", lit("0"))\
        .withColumn("instr_bonus", instr(col("bonus_consumed"), '#')-1)\
        .withColumn("offer_id", F.expr("substring(bonus_consumed,1,instr_bonus)"))\
        .withColumn("trx_lacci",when((F.col('future_string3') == "129"), (F.col('trx_1'))).when((F.col('future_string3') == "130"), (F.col('trx_1'))).otherwise(F.col('trx_2')))\
        .withColumn("lenght_mnc", length(F.col('mcc_mnc')) - 5)\
        .withColumn("mnc", F.expr("substring(mcc_mnc,5,length(mcc_mnc))"))\
        .withColumn("load_user", lit("Solusi24"))\
        .withColumn("subs_id", lit("")).withColumn("cust_type_desc", lit("")).withColumn("cust_subtype_desc", lit("")).withColumn("number_of_network_reroutings", lit("")).withColumn("redirecting_party_id", lit("")).withColumn("pre_call_duration", lit("")).withColumn("lacci_id", lit("")).withColumn("lacci_closing_flag", lit("")).withColumn("lac", lit("")).withColumn("ci", lit("")).withColumn("node_type", lit("")).withColumn("area_sales", lit("")).withColumn("region_sales", lit("")).withColumn("branch", lit("")).withColumn("subbranch", lit("")).withColumn("cluster_sales", lit("")).withColumn("provinsi", lit("")).withColumn("kabupaten", lit("")).withColumn("kecamatan", lit("")).withColumn("kelurahan", lit("")).withColumn("l4_name", lit("")).withColumn("l3_name", lit("")).withColumn("l2_name", lit("")).withColumn("l1_name", lit("")).withColumn("file_id", lit("")).withColumn("load_ts", lit("")).withColumn("load_user", lit("")).withColumn("event_date", lit("")).withColumn("productline_bonus_l4", lit("")).withColumn("retry_ts", lit("")).withColumn("status_data", lit("")).withColumn("status", lit("")).withColumn("key_timestamp_r", lit("")).withColumn("key_a_party", lit("")).withColumn("key_b_party", lit("")).withColumn("key_call_duration", lit("")).withColumn("key_total_volume", lit("")).withColumn("location_number_type_a", lit(""))\
        .withColumn("key_charging_id", lit(""))\
        .withColumn("key_account_delta", lit(F.col('account_delta')))\
        .withColumn("key_bonus_consumed", lit(F.col('bonus_consumed')))\
        .withColumn("msisdn", lit(F.col('a_party')))\
        .withColumn("a_party", lit(F.col('a_party')))\
        .withColumn("account_owner", lit(F.col('event_source')))\
        .withColumn("location_number_a", lit(F.col('locationnumbertypea')))\
        .withColumn("location_number_type_b", lit(F.col('locationnumbertypeb')))\
        .withColumn("location_number_b", lit(F.col('locationnumberb')))\
        .withColumn("rated_date_time", lit(F.col('rateddatetime')))\
        .withColumn("negotiated_qos", lit(F.col('negotiatedqos')))\
        .withColumn("requested_qos", lit(F.col('requestedqos')))\
        .withColumn("subscribed_qos", lit(F.col('subscribedqos')))\
        .withColumn("vlr_num", lit(F.col('vlr_number')))\
        .withColumn("filename", lit("file:///home/hdoop/spark-3.3.1-bin-hadoop3/CHG"))\
        .withColumn("rec_id",row_number().over(Window.orderBy(monotonically_increasing_id())))\
        .select('timestamp_r','trx_date','trx_hour','a_party','account_owner','msisdn','subs_id','cust_type_desc','cust_subtype_desc','location_number_type_a','location_number_a','b_party','location_number_type_b','location_number_b','number_of_reroutings','imsi','number_of_network_reroutings','redirecting_party_id','pre_call_duration','call_duration','charging_id','roaming_flag','vlr_num','cell_id','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','negotiated_qos','requested_qos','subscribed_qos','dialled_apn','eventtype','providerid','currency','subscriber_status','forwarding_flag','first_call_flag','camel_roaming','time_zone','account_balance','account_number','event_source','guiding_resource_type','rounded_duration','total_volume','rounded_total_volume','event_start_period','cifa','account_delta','bonus_consumed','credit_indicator','rev','discount_amount','free_total_volume','free_duration','call_direction','charge_code_description','special_number_ind','bonus_information','internal_cause','basic_cause','time_band','call_type','vas_code','service_filter','national_calling_zone','national_called_zone','international_calling_zone','international_called_zone','event_id','access_code','country_name','cp_name','content_id','rating_group','bft_indicator','unapplied_amount','partition_id','rated_date_time','area','cost_band','l4_name','l3_name','l2_name','l1_name','rating_offer_id','settlement_indicator','tap_code','mcc_mnc','rating_pricing_item_id','file_identifier','record_number','future_string1','future_string2','future_string3','allowance_consumed_revenue','ifrs_ind_for_allowance_revenue','ifrs_ind_for_vas_event','itemized_bill_fee','consumed_monetary_allowance','partial_charge_ind','brand','offer_id','file_id','load_ts','load_user','sitename','pre_post_flag','mcc','mnc','network_activity_id','event_date','productline_bonus_l4','filename','rec_id','retry_count','retry_ts','status_data','status','trx_lacci','key_timestamp_r','key_a_party','key_b_party','key_call_duration','key_charging_id','key_total_volume','key_account_delta','key_bonus_consumed')\
        .withColumn("status", lit ("NODUP"))

SUBS_DIM=hv.table('chg.subs_dim')\
        .withColumnRenamed("subs_id","subs_id_1").withColumnRenamed("msisdn","msisdn_1").withColumnRenamed("cust_type_desc","cust_type_desc_1")\
        .withColumnRenamed("cust_subtype_desc","cust_subtype_desc_1").withColumnRenamed("trx_date","trx_date_1").withColumnRenamed("load_ts","load_ts_1")\
        .select('msisdn_1','subs_id_1','cust_type_desc_1','cust_subtype_desc_1')

join1=proses1.join(SUBS_DIM,proses1.msisdn ==  SUBS_DIM.msisdn_1, how = 'left')\
        .withColumn("subs_id", when((F.col('msisdn') == F.col('msisdn_1')), (F.col('subs_id_1'))).otherwise("-99"))\
        .withColumn("cust_type_desc", when((F.col('msisdn') == F.col('msisdn_1')), (F.col('subs_id_1'))).otherwise("NQ"))\
        .withColumn("cust_subtype_desc", when((F.col('msisdn') == F.col('msisdn_1')), (F.col('cust_subtype_desc_1'))).otherwise("NQ"))        \
        .select('timestamp_r','trx_date','trx_hour','a_party','account_owner','msisdn','subs_id','cust_type_desc','cust_subtype_desc','location_number_type_a','location_number_a','b_party','location_number_type_b','location_number_b','number_of_reroutings','imsi','number_of_network_reroutings','redirecting_party_id','pre_call_duration','call_duration','charging_id','roaming_flag','vlr_num','cell_id','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','negotiated_qos','requested_qos','subscribed_qos','dialled_apn','eventtype','providerid','currency','subscriber_status','forwarding_flag','first_call_flag','camel_roaming','time_zone','account_balance','account_number','event_source','guiding_resource_type','rounded_duration','total_volume','rounded_total_volume','event_start_period','cifa','account_delta','bonus_consumed','credit_indicator','rev','discount_amount','free_total_volume','free_duration','call_direction','charge_code_description','special_number_ind','bonus_information','internal_cause','basic_cause','time_band','call_type','vas_code','service_filter','national_calling_zone','national_called_zone','international_calling_zone','international_called_zone','event_id','access_code','country_name','cp_name','content_id','rating_group','bft_indicator','unapplied_amount','partition_id','rated_date_time','area','cost_band','l4_name','l3_name','l2_name','l1_name','rating_offer_id','settlement_indicator','tap_code','mcc_mnc','rating_pricing_item_id','file_identifier','record_number','future_string1','future_string2','future_string3','allowance_consumed_revenue','ifrs_ind_for_allowance_revenue','ifrs_ind_for_vas_event','itemized_bill_fee','consumed_monetary_allowance','partial_charge_ind','brand','offer_id','file_id','load_ts','load_user','sitename','pre_post_flag','mcc','mnc','network_activity_id','event_date','productline_bonus_l4','filename','rec_id','retry_count','retry_ts','status_data','status','trx_lacci')

MTD=hv.table('chg.mtd')\
    .select('trx_date','subs_id','lacci_id','msisdn','cust_type_desc','cust_subtype_desc','lacci_area_name','lacci_region_network','lacci_cluster_sales','lacci_lac','lacci_ci','lacci_node','lacci_region_sales','lacci_branch','lacci_subbranch','lacci_provinsi','lacci_kabupaten','lacci_kecamatan','lacci_kelurahan')\
    .withColumnRenamed("trx_date","trx_date_1").withColumnRenamed("subs_id","subs_id_1").withColumnRenamed("msisdn","msisdn_1")\
    .withColumnRenamed("lacci_id","lacci_id_mtd").withColumnRenamed("cust_type_desc","cust_type_desc_1").withColumnRenamed("cust_subtype_desc","cust_subtype_desc_1")

join2=join1.join(MTD,(join1.subs_id ==  MTD.subs_id_1) & (join1.msisdn ==  MTD.msisdn_1), how = 'left')\
    .withColumn("mtd_lacci_id", when((F.col('subs_id') == F.col('subs_id_1')), (F.col('lacci_id_mtd'))))\
    .withColumn("mtd_area_name", when((F.col('subs_id') == F.col('subs_id_1')), (F.col('lacci_area_name')))).fillna("UNKNOWN",["mtd_area_name"])\
    .withColumn("mtd_region_network", when((F.col('subs_id') == F.col('subs_id_1')), (F.col('lacci_region_network'))))\
    .withColumn("mtd_cluster_sales", when((F.col('subs_id') == F.col('subs_id_1')), (F.col('lacci_cluster_sales'))))\
    .withColumn("mtd_lac", when((F.col('subs_id') == F.col('subs_id_1')), (F.col('lacci_lac'))))\
    .withColumn("mtd_ci", when((F.col('subs_id') == F.col('subs_id_1')), (F.col('lacci_ci'))))\
    .withColumn("mtd_node", when((F.col('subs_id') == F.col('subs_id_1')), (F.col('lacci_node'))))\
    .withColumn("mtd_region_sales", when((F.col('subs_id') == F.col('subs_id_1')), (F.col('lacci_region_sales'))))\
    .withColumn("mtd_branch", when((F.col('subs_id') == F.col('subs_id_1')), (F.col('lacci_branch'))))\
    .withColumn("mtd_subbranch", when((F.col('subs_id') == F.col('subs_id_1')), (F.col('lacci_subbranch'))))\
    .withColumn("mtd_provinsi", when((F.col('subs_id') == F.col('subs_id_1')), (F.col('lacci_provinsi'))))\
    .withColumn("mtd_kabupaten", when((F.col('subs_id') == F.col('subs_id_1')), (F.col('lacci_kabupaten'))))\
    .withColumn("mtd_kecamatan", when((F.col('subs_id') == F.col('subs_id_1')), (F.col('lacci_kecamatan'))))\
    .withColumn("mtd_kelurahan", when((F.col('subs_id') == F.col('subs_id_1')), (F.col('lacci_kelurahan'))))\
    .withColumn("mtd_flag", when((F.col('subs_id') == F.col('subs_id_1')), ("1")).otherwise("0"))\
    .select('timestamp_r','trx_date','trx_hour','a_party','account_owner','msisdn','subs_id','cust_type_desc','cust_subtype_desc','location_number_type_a','location_number_a','b_party','location_number_type_b','location_number_b','number_of_reroutings','imsi','number_of_network_reroutings','redirecting_party_id','pre_call_duration','call_duration','charging_id','roaming_flag','vlr_num','cell_id','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','negotiated_qos','requested_qos','subscribed_qos','dialled_apn','eventtype','providerid','currency','subscriber_status','forwarding_flag','first_call_flag','camel_roaming','time_zone','account_balance','account_number','event_source','guiding_resource_type','rounded_duration','total_volume','rounded_total_volume','event_start_period','cifa','account_delta','bonus_consumed','credit_indicator','rev','discount_amount','free_total_volume','free_duration','call_direction','charge_code_description','special_number_ind','bonus_information','internal_cause','basic_cause','time_band','call_type','vas_code','service_filter','national_calling_zone','national_called_zone','international_calling_zone','international_called_zone','event_id','access_code','country_name','cp_name','content_id','rating_group','bft_indicator','unapplied_amount','partition_id','rated_date_time','area','cost_band','l4_name','l3_name','l2_name','l1_name','rating_offer_id','settlement_indicator','tap_code','mcc_mnc','rating_pricing_item_id','file_identifier','record_number','future_string1','future_string2','future_string3','allowance_consumed_revenue','ifrs_ind_for_allowance_revenue','ifrs_ind_for_vas_event','itemized_bill_fee','consumed_monetary_allowance','partial_charge_ind','brand','offer_id','file_id','load_ts','load_user','sitename','pre_post_flag','mcc','mnc','network_activity_id','event_date','productline_bonus_l4','filename','rec_id','retry_count','retry_ts','status_data','status','trx_lacci','mtd_lacci_id','mtd_area_name','mtd_region_network','mtd_cluster_sales','mtd_lac','mtd_ci','mtd_node','mtd_region_sales','mtd_branch','mtd_subbranch','mtd_provinsi','mtd_kabupaten','mtd_kecamatan','mtd_kelurahan','mtd_flag')


MM=hv.table('chg.mm')
MM=MM.select('mm_date','subs_id','lacci_id','msisdn','cust_type_desc','cust_subtype_desc','lacci_area_name','lacci_region_network','lacci_cluster_sales','lacci_lac','lacci_ci','lacci_node','lacci_region_sales','lacci_branch','lacci_subbranch','lacci_provinsi','lacci_kabupaten','lacci_kecamatan','lacci_kelurahan')
MM=MM.withColumnRenamed("subs_id","subs_id_1").withColumnRenamed("msisdn","msisdn_1")\
        .withColumnRenamed("lacci_id","lacci_id_mm").withColumnRenamed("cust_type_desc","cust_type_desc_1").withColumnRenamed("cust_subtype_desc","cust_subtype_desc_1")
join3 =join2.join(MM,(join2.subs_id ==  MM.subs_id_1) & (join2.msisdn ==  MM.msisdn_1), how = 'left')\
    .withColumn("prefix", lit("a"))\
    .withColumn("counter",row_number().over(Window.orderBy(monotonically_increasing_id())))\
    .withColumn("trx_lacci", when ((F.col('trx_lacci') == "|") | (F.col('trx_lacci').isNull()),(concat(F.col('prefix'), F.col('counter')))).otherwise(F.col('trx_lacci')))\
    .withColumn("mm_lacci_id", when((F.col('subs_id') == F.col('subs_id_1')), (F.col('lacci_id_mm'))))\
    .withColumn("mm_area_name", when((F.col('subs_id') == F.col('subs_id_1')), (F.col('lacci_area_name')))).fillna("UNKNOWN",["mm_area_name"])\
    .withColumn("mm_region_network", when((F.col('subs_id') == F.col('subs_id_1')), (F.col('lacci_region_network'))))\
    .withColumn("mm_cluster_sales", when((F.col('subs_id') == F.col('subs_id_1')), (F.col('lacci_cluster_sales'))))\
    .withColumn("mm_lac", when((F.col('subs_id') == F.col('subs_id_1')), (F.col('lacci_lac'))))\
    .withColumn("mm_ci", when((F.col('subs_id') == F.col('subs_id_1')), (F.col('lacci_ci'))))\
    .withColumn("mm_node", when((F.col('subs_id') == F.col('subs_id_1')), (F.col('lacci_node'))))\
    .withColumn("mm_region_sales", when((F.col('subs_id') == F.col('subs_id_1')), (F.col('lacci_region_sales'))))\
    .withColumn("mm_branch", when((F.col('subs_id') == F.col('subs_id_1')), (F.col('lacci_branch'))))\
    .withColumn("mm_subbranch", when((F.col('subs_id') == F.col('subs_id_1')), (F.col('lacci_subbranch'))))\
    .withColumn("mm_provinsi", when((F.col('subs_id') == F.col('subs_id_1')), (F.col('lacci_provinsi'))))\
    .withColumn("mm_kabupaten", when((F.col('subs_id') == F.col('subs_id_1')), (F.col('lacci_kabupaten'))))\
    .withColumn("mm_kecamatan", when((F.col('subs_id') == F.col('subs_id_1')), (F.col('lacci_kecamatan'))))\
    .withColumn("mm_kelurahan", when((F.col('subs_id') == F.col('subs_id_1')), (F.col('lacci_kelurahan'))))\
    .withColumn("mm_flag", when((F.col('subs_id') == F.col('subs_id_1')), ("1")).otherwise("0"))\
    .withColumn("mm_status", when((F.col('future_string3') == '129') | (F.col('future_string3') == "130"), "GOOD" ).otherwise("REJECT"))\
    .withColumn("subs_trx", substring(F.col('trx_lacci'), -4,4))
MM_GOOD=join3.filter(col('mm_status')  == 'GOOD').select('timestamp_r','trx_date','trx_hour','a_party','account_owner','msisdn','subs_id','cust_type_desc','cust_subtype_desc','location_number_type_a','location_number_a','b_party','location_number_type_b','location_number_b','number_of_reroutings','imsi','number_of_network_reroutings','redirecting_party_id','pre_call_duration','call_duration','charging_id','roaming_flag','vlr_num','cell_id','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','negotiated_qos','requested_qos','subscribed_qos','dialled_apn','eventtype','providerid','currency','subscriber_status','forwarding_flag','first_call_flag','camel_roaming','time_zone','account_balance','account_number','event_source','guiding_resource_type','rounded_duration','total_volume','rounded_total_volume','event_start_period','cifa','account_delta','bonus_consumed','credit_indicator','rev','discount_amount','free_total_volume','free_duration','call_direction','charge_code_description','special_number_ind','bonus_information','internal_cause','basic_cause','time_band','call_type','vas_code','service_filter','national_calling_zone','national_called_zone','international_calling_zone','international_called_zone','event_id','access_code','country_name','cp_name','content_id','rating_group','bft_indicator','unapplied_amount','partition_id','rated_date_time','area','cost_band','l4_name','l3_name','l2_name','l1_name','rating_offer_id','settlement_indicator','tap_code','mcc_mnc','rating_pricing_item_id','file_identifier','record_number','future_string1','future_string2','future_string3','allowance_consumed_revenue','ifrs_ind_for_allowance_revenue','ifrs_ind_for_vas_event','itemized_bill_fee','consumed_monetary_allowance','partial_charge_ind','brand','offer_id','file_id','load_ts','load_user','sitename','pre_post_flag','mcc','mnc','network_activity_id','event_date','productline_bonus_l4','filename','rec_id','retry_count','retry_ts','status_data','status','trx_lacci','mtd_lacci_id','mtd_area_name','mtd_region_network','mtd_cluster_sales','mtd_lac','mtd_ci','mtd_node','mtd_region_sales','mtd_branch','mtd_subbranch','mtd_provinsi','mtd_kabupaten','mtd_kecamatan','mtd_kelurahan','mtd_flag','mm_lacci_id','mm_area_name','mm_region_network','mm_cluster_sales','mm_lac','mm_ci','mm_node','mm_region_sales','mm_branch','mm_subbranch','mm_provinsi','mm_kabupaten','mm_kecamatan','mm_kelurahan','mm_flag','subs_trx')
MM_REJECT=join3.filter(col('mm_status')  == 'REJECT').select('timestamp_r','trx_date','trx_hour','a_party','account_owner','msisdn','subs_id','cust_type_desc','cust_subtype_desc','location_number_type_a','location_number_a','b_party','location_number_type_b','location_number_b','number_of_reroutings','imsi','number_of_network_reroutings','redirecting_party_id','pre_call_duration','call_duration','charging_id','roaming_flag','vlr_num','cell_id','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','negotiated_qos','requested_qos','subscribed_qos','dialled_apn','eventtype','providerid','currency','subscriber_status','forwarding_flag','first_call_flag','camel_roaming','time_zone','account_balance','account_number','event_source','guiding_resource_type','rounded_duration','total_volume','rounded_total_volume','event_start_period','cifa','account_delta','bonus_consumed','credit_indicator','rev','discount_amount','free_total_volume','free_duration','call_direction','charge_code_description','special_number_ind','bonus_information','internal_cause','basic_cause','time_band','call_type','vas_code','service_filter','national_calling_zone','national_called_zone','international_calling_zone','international_called_zone','event_id','access_code','country_name','cp_name','content_id','rating_group','bft_indicator','unapplied_amount','partition_id','rated_date_time','area','cost_band','l4_name','l3_name','l2_name','l1_name','rating_offer_id','settlement_indicator','tap_code','mcc_mnc','rating_pricing_item_id','file_identifier','record_number','future_string1','future_string2','future_string3','allowance_consumed_revenue','ifrs_ind_for_allowance_revenue','ifrs_ind_for_vas_event','itemized_bill_fee','consumed_monetary_allowance','partial_charge_ind','brand','offer_id','file_id','load_ts','load_user','sitename','pre_post_flag','mcc','mnc','network_activity_id','event_date','productline_bonus_l4','filename','rec_id','retry_count','retry_ts','status_data','status','trx_lacci','mtd_lacci_id','mtd_area_name','mtd_region_network','mtd_cluster_sales','mtd_lac','mtd_ci','mtd_node','mtd_region_sales','mtd_branch','mtd_subbranch','mtd_provinsi','mtd_kabupaten','mtd_kecamatan','mtd_kelurahan','mtd_flag','mm_lacci_id','mm_area_name','mm_region_network','mm_cluster_sales','mm_lac','mm_ci','mm_node','mm_region_sales','mm_branch','mm_subbranch','mm_provinsi','mm_kabupaten','mm_kecamatan','mm_kelurahan','mm_flag','subs_trx')

LACIMMA=hv.table('chg.lacimma')\
        .withColumn('subs_eci', substring(F.col('eci_1'), -4,4))\
        .withColumn('subs_lacci', substring(F.col('lacci_id_1'), -4,4))\
        .withColumn('concat_eci',(concat(col('eci_1'), lit("_"), col('subs_eci'))))\
        .withColumn('concat_lacci',(concat(col('lacci_id_1'), lit("_"), col('subs_lacci'))))
        
join4=MM_GOOD.join(LACIMMA,MM_GOOD.subs_trx ==  LACIMMA.concat_eci, how = 'left')
JOIN_ECI=join4.withColumn('lacci_id', when((join4.trx_lacci == join4.eci_1) & (join4.area_sales_1 != "UNKNOWN"), (join4.lacci_id_1))\
                .when((join4.mtd_flag == "1") & (join4.mtd_area_name != "UNKNOWN"), (join4.mtd_lacci_id))\
                .when((join4.mm_flag == "1") & (join4.mm_area_name != "UNKNOWN"), (join4.mm_lacci_id)).otherwise("-99"))\
        .withColumn('lacci_closing_flag', when((join4.trx_lacci == join4.eci_1) & (join4.area_sales_1 != "UNKNOWN"), ("0"))\
                .when((join4.mtd_flag == "1") & (join4.mtd_area_name != "UNKNOWN"), ("1"))\
                .when((join4.mm_flag == "1") & (join4.mm_area_name != "UNKNOWN"), ("2")).otherwise("-1") )\
        .withColumn('lac', when((join4.trx_lacci == join4.eci_1), (join4.lac_1))\
                .when((join4.mtd_flag == "1"), (join4.mtd_lac))\
                .when((join4.mm_flag == "1"), (join4.mm_lac)).otherwise("NQ"))\
        .withColumn('ci', when((join4.trx_lacci == join4.eci_1), (join4.ci_1))\
                .when((join4.mtd_flag == "1"), (join4.mtd_ci))\
                .when((join4.mm_flag == "1"), (join4.mm_ci)).otherwise("NQ"))\
        .withColumn('node_type', when((join4.trx_lacci == join4.eci_1), (join4.node_1))\
                .when((join4.mtd_flag == "1"), (join4.mtd_node))\
                .when((join4.mm_flag == "1"), (join4.mm_node)).otherwise("NQ"))\
        .withColumn('area_sales', when((join4.trx_lacci == join4.eci_1), (join4.area_sales_1))\
                .when((join4.mtd_flag == "1"), (join4.mtd_area_name))\
                .when((join4.mm_flag == "1"), (join4.mm_area_name)).otherwise("NQ"))\
        .withColumn('region_sales', when((join4.trx_lacci == join4.eci_1), (join4.region_sales_1))\
                .when((join4.mtd_flag == "1"), (join4.mtd_region_sales))\
                .when((join4.mm_flag == "1"), (join4.mm_region_sales)).otherwise("NQ"))\
        .withColumn('branch', when((join4.trx_lacci == join4.eci_1), (join4.branch_1))\
                .when((join4.mtd_flag == "1"), (join4.mtd_branch))\
                .when((join4.mm_flag == "1"), (join4.mm_branch)).otherwise("NQ"))\
        .withColumn('subbranch', when((join4.trx_lacci == join4.eci_1), (join4.subbranch_1))\
                .when((join4.mtd_flag == "1"), (join4.mtd_subbranch))\
                .when((join4.mm_flag == "1"), (join4.mm_subbranch)).otherwise("NQ"))\
        .withColumn('cluster_sales', when((join4.trx_lacci == join4.eci_1), (join4.cluster_sales_1))\
                .when((join4.mtd_flag == "1"), (join4.mtd_cluster_sales))\
                .when((join4.mm_flag == "1"), (join4.mm_cluster_sales)).otherwise("NQ"))\
        .withColumn('provinsi', when((join4.trx_lacci == join4.eci_1), (join4.provinsi_1))\
                .when((join4.mtd_flag == "1"), (join4.mtd_provinsi))\
                .when((join4.mm_flag == "1"), (join4.mm_provinsi)).otherwise("NQ"))\
        .withColumn('kabupaten', when((join4.trx_lacci == join4.eci_1), (join4.kabupaten_1))\
                .when((join4.mtd_flag == "1"), (join4.mtd_kabupaten))\
                .when((join4.mm_flag == "1"), (join4.mm_kabupaten)).otherwise("NQ"))\
        .withColumn('kecamatan', when((join4.trx_lacci == join4.eci_1), (join4.kecamatan_1))\
                .when((join4.mtd_flag == "1"), (join4.mtd_kecamatan))\
                .when((join4.mm_flag == "1"), (join4.mm_kecamatan)).otherwise("NQ"))\
        .withColumn('kelurahan', when((join4.trx_lacci == join4.eci_1), (join4.kelurahan_1))\
                .when((join4.mtd_flag == "1"), (join4.mtd_kelurahan))\
                .when((join4.mm_flag == "1"), (join4.mm_kelurahan)).otherwise("NQ"))
#join4=join4.filter(join4.trx_lacci == join4.eci)
#join4 = join4.select('lacci_id')

join5=MM_REJECT.join(LACIMMA,MM_REJECT.subs_trx ==  LACIMMA.concat_lacci, how = 'left')
JOIN_LACCI=join5.withColumn('lacci_id', when((join5.trx_lacci == join5.lacci_id_1) & (join5.area_sales_1 != "UNKNOWN"), (join5.lacci_id_1))\
                .when((join5.mtd_flag == "1") & (join5.mtd_area_name != "UNKNOWN"), (join5.mtd_lacci_id))\
                .when((join5.mm_flag == "1") & (join5.mm_area_name != "UNKNOWN"), (join5.mm_lacci_id)).otherwise("-99"))\
        .withColumn('lacci_closing_flag', when((join5.trx_lacci == join5.lacci_id_1) & (join5.area_sales_1 != "UNKNOWN"), ("0"))\
                .when((join5.mtd_flag == "1") & (join5.mtd_area_name != "UNKNOWN"), ("1"))\
                .when((join5.mm_flag == "1") & (join5.mm_area_name != "UNKNOWN"), ("2")).otherwise("-1") )\
        .withColumn('lac', when((join5.trx_lacci == join5.lacci_id_1), (join5.lac_1))\
                .when((join5.mtd_flag == "1"), (join5.mtd_lac))\
                .when((join5.mm_flag == "1"), (join5.mm_lac)).otherwise("NQ"))\
        .withColumn('ci', when((join5.trx_lacci == join5.lacci_id_1), (join5.ci_1))\
                .when((join5.mtd_flag == "1"), (join5.mtd_ci))\
                .when((join5.mm_flag == "1"), (join5.mm_ci)).otherwise("NQ"))\
        .withColumn('node_type', when((join5.trx_lacci == join5.lacci_id_1), (join5.node_1))\
                .when((join5.mtd_flag == "1"), (join5.mtd_node))\
                .when((join5.mm_flag == "1"), (join5.mm_node)).otherwise("NQ"))\
        .withColumn('area_sales', when((join5.trx_lacci == join5.lacci_id_1), (join5.area_sales_1))\
                .when((join5.mtd_flag == "1"), (join5.mtd_area_name))\
                .when((join5.mm_flag == "1"), (join5.mm_area_name)).otherwise("NQ"))\
        .withColumn('region_sales', when((join5.trx_lacci == join5.lacci_id_1), (join5.region_sales_1))\
                .when((join5.mtd_flag == "1"), (join5.mtd_region_sales))\
                .when((join5.mm_flag == "1"), (join5.mm_region_sales)).otherwise("NQ"))\
        .withColumn('branch', when((join5.trx_lacci == join5.lacci_id_1), (join5.branch_1))\
                .when((join5.mtd_flag == "1"), (join5.mtd_branch))\
                .when((join5.mm_flag == "1"), (join5.mm_branch)).otherwise("NQ"))\
        .withColumn('subbranch', when((join5.trx_lacci == join5.lacci_id_1), (join5.subbranch_1))\
                .when((join5.mtd_flag == "1"), (join5.mtd_subbranch))\
                .when((join5.mm_flag == "1"), (join5.mm_subbranch)).otherwise("NQ"))\
        .withColumn('cluster_sales', when((join5.trx_lacci == join5.lacci_id_1), (join5.cluster_sales_1))\
                .when((join5.mtd_flag == "1"), (join5.mtd_cluster_sales))\
                .when((join5.mm_flag == "1"), (join5.mm_cluster_sales)).otherwise("NQ"))\
        .withColumn('provinsi', when((join5.trx_lacci == join5.lacci_id_1), (join5.provinsi_1))\
                .when((join5.mtd_flag == "1"), (join5.mtd_provinsi))\
                .when((join5.mm_flag == "1"), (join5.mm_provinsi)).otherwise("NQ"))\
        .withColumn('kabupaten', when((join5.trx_lacci == join5.lacci_id_1), (join5.kabupaten_1))\
                .when((join5.mtd_flag == "1"), (join5.mtd_kabupaten))\
                .when((join5.mm_flag == "1"), (join5.mm_kabupaten)).otherwise("NQ"))\
        .withColumn('kecamatan', when((join5.trx_lacci == join5.lacci_id_1), (join5.kecamatan_1))\
                .when((join5.mtd_flag == "1"), (join5.mtd_kecamatan))\
                .when((join5.mm_flag == "1"), (join5.mm_kecamatan)).otherwise("NQ"))\
        .withColumn('kelurahan', when((join5.trx_lacci == join5.lacci_id_1), (join5.kelurahan_1))\
                .when((join5.mtd_flag == "1"), (join5.mtd_kelurahan))\
                .when((join5.mm_flag == "1"), (join5.mm_kelurahan)).otherwise("NQ"))

from functools import reduce 
df = [ JOIN_ECI,JOIN_LACCI]
merge=reduce(DataFrame.unionAll, df)\
    .select('timestamp_r','trx_date','trx_hour','a_party','account_owner','msisdn','subs_id','cust_type_desc','cust_subtype_desc','location_number_type_a','location_number_a','b_party','location_number_type_b','location_number_b','number_of_reroutings','imsi','number_of_network_reroutings','redirecting_party_id','pre_call_duration','call_duration','charging_id','roaming_flag','vlr_num','cell_id','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','negotiated_qos','requested_qos','subscribed_qos','dialled_apn','eventtype','providerid','currency','subscriber_status','forwarding_flag','first_call_flag','camel_roaming','time_zone','account_balance','account_number','event_source','guiding_resource_type','rounded_duration','total_volume','rounded_total_volume','event_start_period','cifa','account_delta','bonus_consumed','credit_indicator','rev','discount_amount','free_total_volume','free_duration','call_direction','charge_code_description','special_number_ind','bonus_information','internal_cause','basic_cause','time_band','call_type','vas_code','service_filter','national_calling_zone','national_called_zone','international_calling_zone','international_called_zone','event_id','access_code','country_name','cp_name','content_id','rating_group','bft_indicator','unapplied_amount','partition_id','rated_date_time','area','cost_band','l4_name','l3_name','l2_name','l1_name','rating_offer_id','settlement_indicator','tap_code','mcc_mnc','rating_pricing_item_id','file_identifier','record_number','future_string1','future_string2','future_string3','allowance_consumed_revenue','ifrs_ind_for_allowance_revenue','ifrs_ind_for_vas_event','itemized_bill_fee','consumed_monetary_allowance','partial_charge_ind','brand','offer_id','file_id','load_ts','load_user','sitename','pre_post_flag','mcc','mnc','network_activity_id','event_date','productline_bonus_l4','filename','rec_id','retry_count','retry_ts','status_data','status','trx_lacci')\
    .withColumn('trx_lacci', when((F.col('trx_lacci').like ('a')),"").otherwise(F.col('trx_lacci')))
    
merge.write.csv('file:///home/hdoop/spark-3.3.1-bin-hadoop3/USAGE_CHG_HIVE_SATU',sep ='~')

####
hv.sql("use CHG_1")
hv.sql("CREATE TABLE IF NOT EXISTS CHG_PART_1 (timestamp_r string,trx_date string,trx_hour string,a_party string,account_owner string,msisdn string,subs_id string,cust_type_desc string,cust_subtype_desc string,location_number_type_a string,location_number_a string,b_party string,location_number_type_b string,location_number_b string,number_of_reroutings string,imsi string,number_of_network_reroutings string,redirecting_party_id string,pre_call_duration string,call_duration string,charging_id string,roaming_flag string,vlr_num string,cell_id string,lacci_id string,lacci_closing_flag string,lac string,ci string,node_type string,area_sales string,region_sales string,branch string,subbranch string,cluster_sales string,provinsi string,kabupaten string,kecamatan string,kelurahan string,negotiated_qos string,requested_qos string,subscribed_qos string,dialled_apn string,eventtype string,providerid string,currency string,subscriber_status string,forwarding_flag string,first_call_flag string,camel_roaming string,time_zone string,account_balance string,account_number string,event_source string,guiding_resource_type string,rounded_duration string,total_volume string,rounded_total_volume string,event_start_period string,cifa string,account_delta string,bonus_consumed string,credit_indicator string,rev string,discount_amount string,free_total_volume string,free_duration string,call_direction string,charge_code_description string,special_number_ind string,bonus_information string,internal_cause string,basic_cause string,time_band string,call_type string,vas_code string,service_filter string,national_calling_zone string,national_called_zone string,international_calling_zone string,international_called_zone string,event_id string,access_code string,country_name string,cp_name string,content_id string,rating_group string,bft_indicator string,unapplied_amount string,partition_id string,rated_date_time string,area string,cost_band string,l4_name string,l3_name string,l2_name string,l1_name string,rating_offer_id string,settlement_indicator string,tap_code string,mcc_mnc string,rating_pricing_item_id string,file_identifier string,record_number string,future_string1 string,future_string2 string,future_string3 string,allowance_consumed_revenue string,ifrs_ind_for_allowance_revenue string,ifrs_ind_for_vas_event string,itemized_bill_fee string,consumed_monetary_allowance string,partial_charge_ind string,brand string,offer_id string,file_id string,load_ts string,load_user string,sitename string,pre_post_flag string,mcc string,mnc string,network_activity_id string,event_date string,productline_bonus_l4 string,filename string,rec_id string,retry_count string,retry_ts string,status_data string,status string,trx_lacci string)\
        row format delimited fields terminated by '~'\
    ")
hv.sql("load data local inpath 'file:///home/hdoop/spark-3.3.1-bin-hadoop3/USAGE_CHG_HIVE_SATU' overwrite into table CHG_PART_1")
end_date = datetime.now() + timedelta(hours=7)
duration = (end_date - start_date).seconds
###
print(start_date.strftime('%Y-%m-%d %H:%M:%S'))
print(end_date.strftime('%Y-%m-%d %H:%M:%S'))
print(duration)