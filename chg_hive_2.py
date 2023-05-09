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
from pyspark.sql.functions import rank

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

hv.sql("use CHG_1")
hv.sql("CREATE TABLE IF NOT EXISTS product_lime (trx_date string,rkey string,mapping_key_type string,service_type string,l1_name string,l2_name string,l3_name string,l4_name string,price string,updated_date string,product_name string,plan_type string,plan_service string,quota_value string,quota_unit string,validity_value string,validity_unit string,vascode_type string,file_id string,load_ts string,load_user string,event_date string,product_category string,future_string_1 string,future_string_2 string,future_string_3 string)\
    STORED AS PARQUET;\
    ")
hv.sql("load data local inpath 'file:///home/hdoop/j_input' overwrite into table product_lime")
hv.sql("CREATE TABLE IF NOT EXISTS bonus (product_name string,price string,l1 string,l2 string,l3 string,l4 string,note string,source string)\
    row format delimited fields terminated by '|'\
    ")
hv.sql("load data local inpath 'file:///home/hdoop/PRODUCT_BONUS.txt' overwrite into table bonus")
###
usage1=hv.table('CHG_1.CHG_PART_1')
product_lime=hv.table('CHG_1.product_lime')\
    .select('mapping_key_type','service_type','l1_name','l4_name','price','updated_date','quota_value','quota_unit','rkey','l2_name','l3_name','product_name','plan_type','plan_service','validity_value','validity_unit','vascode_type')\
    .withColumnRenamed('l4_name', 'l4_name_1')\
    .withColumnRenamed('l3_name', 'l3_name_1')\
    .withColumnRenamed('l2_name', 'l2_name_1')\
    .withColumnRenamed('l1_name', 'l1_name_1')

filter_service=usage1.withColumn('service', when(F.col("service_filter").startswith("1_"), "SERVICE1")\
    .when(F.col("service_filter").startswith("2_"), "SERVICE2")\
    .when(F.col("service_filter").startswith("3_"), "SERVICE3")\
    .when(F.col("service_filter").startswith("5_"), "SERVICE5").otherwise ("SERVICE_REJECT"))

service1=filter_service.filter(F.col("service") == "SERVICE1")
service2=filter_service.filter(F.col("service") == "SERVICE2")
service3=filter_service.filter(F.col("service") == "SERVICE3")
service5=filter_service.filter(F.col("service") == "SERVICE5")
service_reject=filter_service.filter(F.col("service") == "SERVICE_REJECT")

HASIL1=service1.join(product_lime,service1.offer_id ==  product_lime.rkey, how = 'left')\
    .withColumn('l4_name', when((F.col('offer_id') ==  F.col('rkey')) & (F.col('mapping_key_type').isNull()), (F.col('l4_name_1'))).otherwise ("NQ"))\
    .withColumn('l3_name', when((F.col('offer_id') ==  F.col('rkey')) & (F.col('mapping_key_type').isNull()), (F.col('l3_name_1'))).otherwise ("NQ"))\
    .withColumn('l2_name', when((F.col('offer_id') ==  F.col('rkey')) & (F.col('mapping_key_type').isNull()), (F.col('l2_name_1'))).otherwise ("NQ"))\
    .withColumn('l1_name', when((F.col('offer_id') ==  F.col('rkey')) & (F.col('mapping_key_type').isNull()), (F.col('l1_name_1'))).otherwise ("NQ"))\
    .select('timestamp_r','trx_date','trx_hour','a_party','account_owner','msisdn','subs_id','cust_type_desc','cust_subtype_desc','location_number_type_a','location_number_a','b_party','location_number_type_b','location_number_b','number_of_reroutings','imsi','number_of_network_reroutings','redirecting_party_id','pre_call_duration','call_duration','charging_id','roaming_flag','vlr_num','cell_id','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','negotiated_qos','requested_qos','subscribed_qos','dialled_apn','eventtype','providerid','currency','subscriber_status','forwarding_flag','first_call_flag','camel_roaming','time_zone','account_balance','account_number','event_source','guiding_resource_type','rounded_duration','total_volume','rounded_total_volume','event_start_period','cifa','account_delta','bonus_consumed','credit_indicator','rev','discount_amount','free_total_volume','free_duration','call_direction','charge_code_description','special_number_ind','bonus_information','internal_cause','basic_cause','time_band','call_type','vas_code','service_filter','national_calling_zone','national_called_zone','international_calling_zone','international_called_zone','event_id','access_code','country_name','cp_name','content_id','rating_group','bft_indicator','unapplied_amount','partition_id','rated_date_time','area','cost_band','l4_name','l3_name','l2_name','l1_name','rating_offer_id','settlement_indicator','tap_code','mcc_mnc','rating_pricing_item_id','file_identifier','record_number','future_string1','future_string2','future_string3','allowance_consumed_revenue','ifrs_ind_for_allowance_revenue','ifrs_ind_for_vas_event','itemized_bill_fee','consumed_monetary_allowance','partial_charge_ind','brand','offer_id','file_id','load_ts','load_user','sitename','pre_post_flag','mcc','mnc','network_activity_id','event_date','productline_bonus_l4','filename','rec_id','retry_count','retry_ts','status_data','status','trx_lacci')


join2=service2.join(product_lime,service2.b_party ==  product_lime.rkey, how = 'left')\
    .withColumn('l4_name', when((F.col('b_party') == F.col('rkey')) & (F.col('service_type') == "2_") & (F.col('product_name').isNotNull()), (F.col('l4_name_1'))).otherwise("NQ"))\
    .withColumn('l3_name', when((F.col('b_party') == F.col('rkey')) & (F.col('service_type') == "2_") & (F.col('product_name').isNotNull()), (F.col('l3_name_1'))).otherwise("NQ"))\
    .withColumn('l2_name', when((F.col('b_party') == F.col('rkey')) & (F.col('service_type') == "2_") & (F.col('product_name').isNotNull()), (F.col('l2_name_1'))).otherwise("NQ"))\
    .withColumn('l1_name', when((F.col('b_party') == F.col('rkey')) & (F.col('service_type') == "2_") & (F.col('product_name').isNotNull()), (F.col('l1_name_1'))).otherwise("NQ"))\
    .select('timestamp_r','trx_date','trx_hour','a_party','account_owner','msisdn','subs_id','cust_type_desc','cust_subtype_desc','location_number_type_a','location_number_a','b_party','location_number_type_b','location_number_b','number_of_reroutings','imsi','number_of_network_reroutings','redirecting_party_id','pre_call_duration','call_duration','charging_id','roaming_flag','vlr_num','cell_id','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','negotiated_qos','requested_qos','subscribed_qos','dialled_apn','eventtype','providerid','currency','subscriber_status','forwarding_flag','first_call_flag','camel_roaming','time_zone','account_balance','account_number','event_source','guiding_resource_type','rounded_duration','total_volume','rounded_total_volume','event_start_period','cifa','account_delta','bonus_consumed','credit_indicator','rev','discount_amount','free_total_volume','free_duration','call_direction','charge_code_description','special_number_ind','bonus_information','internal_cause','basic_cause','time_band','call_type','vas_code','service_filter','national_calling_zone','national_called_zone','international_calling_zone','international_called_zone','event_id','access_code','country_name','cp_name','content_id','rating_group','bft_indicator','unapplied_amount','partition_id','rated_date_time','area','cost_band','l4_name','l3_name','l2_name','l1_name','rating_offer_id','settlement_indicator','tap_code','mcc_mnc','rating_pricing_item_id','file_identifier','record_number','future_string1','future_string2','future_string3','allowance_consumed_revenue','ifrs_ind_for_allowance_revenue','ifrs_ind_for_vas_event','itemized_bill_fee','consumed_monetary_allowance','partial_charge_ind','brand','offer_id','file_id','load_ts','load_user','sitename','pre_post_flag','mcc','mnc','network_activity_id','event_date','productline_bonus_l4','filename','rec_id','retry_count','retry_ts','status_data','status','trx_lacci')

join3=join2.join(product_lime,join2.offer_id ==  product_lime.rkey, how = 'left')\
    .withColumn('l4_name', when((F.col('l4_name') == "NQ") &(F.col('offer_id') == F.col('rkey')) & (F.col('service_type') == "2_") & (F.col('product_name').isNotNull()), (F.col('l4_name_1'))).otherwise("NQ"))\
    .withColumn('l3_name', when((F.col('l3_name') == "NQ") &(F.col('offer_id') == F.col('rkey')) & (F.col('service_type') == "2_") & (F.col('product_name').isNotNull()), (F.col('l3_name_1'))).otherwise("NQ"))\
    .withColumn('l2_name', when((F.col('l2_name') == "NQ") &(F.col('offer_id') == F.col('rkey')) & (F.col('service_type') == "2_") & (F.col('product_name').isNotNull()), (F.col('l2_name_1'))).otherwise("NQ"))\
    .withColumn('l1_name', when((F.col('l1_name') == "NQ") &(F.col('offer_id') == F.col('rkey')) & (F.col('service_type') == "2_") & (F.col('product_name').isNotNull()), (F.col('l1_name_1'))).otherwise("NQ"))  \
    .select('timestamp_r','trx_date','trx_hour','a_party','account_owner','msisdn','subs_id','cust_type_desc','cust_subtype_desc','location_number_type_a','location_number_a','b_party','location_number_type_b','location_number_b','number_of_reroutings','imsi','number_of_network_reroutings','redirecting_party_id','pre_call_duration','call_duration','charging_id','roaming_flag','vlr_num','cell_id','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','negotiated_qos','requested_qos','subscribed_qos','dialled_apn','eventtype','providerid','currency','subscriber_status','forwarding_flag','first_call_flag','camel_roaming','time_zone','account_balance','account_number','event_source','guiding_resource_type','rounded_duration','total_volume','rounded_total_volume','event_start_period','cifa','account_delta','bonus_consumed','credit_indicator','rev','discount_amount','free_total_volume','free_duration','call_direction','charge_code_description','special_number_ind','bonus_information','internal_cause','basic_cause','time_band','call_type','vas_code','service_filter','national_calling_zone','national_called_zone','international_calling_zone','international_called_zone','event_id','access_code','country_name','cp_name','content_id','rating_group','bft_indicator','unapplied_amount','partition_id','rated_date_time','area','cost_band','l4_name','l3_name','l2_name','l1_name','rating_offer_id','settlement_indicator','tap_code','mcc_mnc','rating_pricing_item_id','file_identifier','record_number','future_string1','future_string2','future_string3','allowance_consumed_revenue','ifrs_ind_for_allowance_revenue','ifrs_ind_for_vas_event','itemized_bill_fee','consumed_monetary_allowance','partial_charge_ind','brand','offer_id','file_id','load_ts','load_user','sitename','pre_post_flag','mcc','mnc','network_activity_id','event_date','productline_bonus_l4','filename','rec_id','retry_count','retry_ts','status_data','status','trx_lacci')

HASIL2=join3.join(product_lime,join3.service_filter ==  product_lime.rkey, how = 'left')\
    .withColumn('l4_name', when((F.col('l4_name') == "NQ") &(F.col('service_filter') == F.col('rkey')) & (F.col('service_type') == "2_") & (F.col('product_name').isNotNull()), (F.col('l4_name_1'))).otherwise("NQ"))\
    .withColumn('l3_name', when((F.col('l3_name') == "NQ") & (F.col('service_filter') == F.col('rkey')) & (F.col('service_type') == "2_") & (F.col('product_name').isNotNull()), (F.col('l3_name_1'))).otherwise("NQ"))\
    .withColumn('l2_name', when((F.col('l2_name') == "NQ") & (F.col('service_filter') == F.col('rkey')) & (F.col('service_type') == "2_") & (F.col('product_name').isNotNull()), (F.col('l2_name_1'))).otherwise("NQ"))\
    .withColumn('l1_name', when((F.col('l1_name') == "NQ") & (F.col('service_filter') == F.col('rkey')) & (F.col('service_type') == "2_") & (F.col('product_name').isNotNull()), (F.col('l3_name_1'))).otherwise("NQ"))\
    .select('timestamp_r','trx_date','trx_hour','a_party','account_owner','msisdn','subs_id','cust_type_desc','cust_subtype_desc','location_number_type_a','location_number_a','b_party','location_number_type_b','location_number_b','number_of_reroutings','imsi','number_of_network_reroutings','redirecting_party_id','pre_call_duration','call_duration','charging_id','roaming_flag','vlr_num','cell_id','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','negotiated_qos','requested_qos','subscribed_qos','dialled_apn','eventtype','providerid','currency','subscriber_status','forwarding_flag','first_call_flag','camel_roaming','time_zone','account_balance','account_number','event_source','guiding_resource_type','rounded_duration','total_volume','rounded_total_volume','event_start_period','cifa','account_delta','bonus_consumed','credit_indicator','rev','discount_amount','free_total_volume','free_duration','call_direction','charge_code_description','special_number_ind','bonus_information','internal_cause','basic_cause','time_band','call_type','vas_code','service_filter','national_calling_zone','national_called_zone','international_calling_zone','international_called_zone','event_id','access_code','country_name','cp_name','content_id','rating_group','bft_indicator','unapplied_amount','partition_id','rated_date_time','area','cost_band','l4_name','l3_name','l2_name','l1_name','rating_offer_id','settlement_indicator','tap_code','mcc_mnc','rating_pricing_item_id','file_identifier','record_number','future_string1','future_string2','future_string3','allowance_consumed_revenue','ifrs_ind_for_allowance_revenue','ifrs_ind_for_vas_event','itemized_bill_fee','consumed_monetary_allowance','partial_charge_ind','brand','offer_id','file_id','load_ts','load_user','sitename','pre_post_flag','mcc','mnc','network_activity_id','event_date','productline_bonus_l4','filename','rec_id','retry_count','retry_ts','status_data','status','trx_lacci')


product_lime=product_lime.dropDuplicates(['rkey'])
join5=service3.join(product_lime,service3.b_party ==  product_lime.rkey, how = 'left')\
    .withColumn('l4_name', when((F.col('b_party') == F.col('rkey')) & (F.col('service_type') == "3_") & (F.col('product_name').isNotNull()), (F.col('l4_name_1'))).otherwise("NQ"))\
    .withColumn('l3_name', when((F.col('b_party') == F.col('rkey')) & (F.col('service_type') == "3_") & (F.col('product_name').isNotNull()), (F.col('l3_name_1'))).otherwise("NQ"))\
    .withColumn('l2_name', when((F.col('b_party') == F.col('rkey')) & (F.col('service_type') == "3_") & (F.col('product_name').isNotNull()), (F.col('l2_name_1'))).otherwise("NQ"))\
    .withColumn('l1_name', when((F.col('b_party') == F.col('rkey')) & (F.col('service_type') == "3_") & (F.col('product_name').isNotNull()), (F.col('l1_name_1'))).otherwise("NQ"))\
    .select('timestamp_r','trx_date','trx_hour','a_party','account_owner','msisdn','subs_id','cust_type_desc','cust_subtype_desc','location_number_type_a','location_number_a','b_party','location_number_type_b','location_number_b','number_of_reroutings','imsi','number_of_network_reroutings','redirecting_party_id','pre_call_duration','call_duration','charging_id','roaming_flag','vlr_num','cell_id','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','negotiated_qos','requested_qos','subscribed_qos','dialled_apn','eventtype','providerid','currency','subscriber_status','forwarding_flag','first_call_flag','camel_roaming','time_zone','account_balance','account_number','event_source','guiding_resource_type','rounded_duration','total_volume','rounded_total_volume','event_start_period','cifa','account_delta','bonus_consumed','credit_indicator','rev','discount_amount','free_total_volume','free_duration','call_direction','charge_code_description','special_number_ind','bonus_information','internal_cause','basic_cause','time_band','call_type','vas_code','service_filter','national_calling_zone','national_called_zone','international_calling_zone','international_called_zone','event_id','access_code','country_name','cp_name','content_id','rating_group','bft_indicator','unapplied_amount','partition_id','rated_date_time','area','cost_band','l4_name','l3_name','l2_name','l1_name','rating_offer_id','settlement_indicator','tap_code','mcc_mnc','rating_pricing_item_id','file_identifier','record_number','future_string1','future_string2','future_string3','allowance_consumed_revenue','ifrs_ind_for_allowance_revenue','ifrs_ind_for_vas_event','itemized_bill_fee','consumed_monetary_allowance','partial_charge_ind','brand','offer_id','file_id','load_ts','load_user','sitename','pre_post_flag','mcc','mnc','network_activity_id','event_date','productline_bonus_l4','filename','rec_id','retry_count','retry_ts','status_data','status','trx_lacci')

join6=join5.join(product_lime,join5.offer_id ==  product_lime.rkey, how = 'left')\
    .withColumn('l4_name', when((F.col('l4_name') == "NQ") &(F.col('offer_id') == F.col('rkey')) & (F.col('service_type') == "3_") & (F.col('product_name').isNotNull()), (F.col('l4_name_1'))).otherwise("NQ"))\
    .withColumn('l3_name', when((F.col('l3_name') == "NQ") &(F.col('offer_id') == F.col('rkey')) & (F.col('service_type') == "3_") & (F.col('product_name').isNotNull()), (F.col('l3_name_1'))).otherwise("NQ"))\
    .withColumn('l2_name', when((F.col('l2_name') == "NQ") &(F.col('offer_id') == F.col('rkey')) & (F.col('service_type') == "3_") & (F.col('product_name').isNotNull()), (F.col('l2_name_1'))).otherwise("NQ"))\
    .withColumn('l1_name', when((F.col('l1_name') == "NQ") &(F.col('offer_id') == F.col('rkey')) & (F.col('service_type') == "3_") & (F.col('product_name').isNotNull()), (F.col('l1_name_1'))).otherwise("NQ"))\
    .select('timestamp_r','trx_date','trx_hour','a_party','account_owner','msisdn','subs_id','cust_type_desc','cust_subtype_desc','location_number_type_a','location_number_a','b_party','location_number_type_b','location_number_b','number_of_reroutings','imsi','number_of_network_reroutings','redirecting_party_id','pre_call_duration','call_duration','charging_id','roaming_flag','vlr_num','cell_id','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','negotiated_qos','requested_qos','subscribed_qos','dialled_apn','eventtype','providerid','currency','subscriber_status','forwarding_flag','first_call_flag','camel_roaming','time_zone','account_balance','account_number','event_source','guiding_resource_type','rounded_duration','total_volume','rounded_total_volume','event_start_period','cifa','account_delta','bonus_consumed','credit_indicator','rev','discount_amount','free_total_volume','free_duration','call_direction','charge_code_description','special_number_ind','bonus_information','internal_cause','basic_cause','time_band','call_type','vas_code','service_filter','national_calling_zone','national_called_zone','international_calling_zone','international_called_zone','event_id','access_code','country_name','cp_name','content_id','rating_group','bft_indicator','unapplied_amount','partition_id','rated_date_time','area','cost_band','l4_name','l3_name','l2_name','l1_name','rating_offer_id','settlement_indicator','tap_code','mcc_mnc','rating_pricing_item_id','file_identifier','record_number','future_string1','future_string2','future_string3','allowance_consumed_revenue','ifrs_ind_for_allowance_revenue','ifrs_ind_for_vas_event','itemized_bill_fee','consumed_monetary_allowance','partial_charge_ind','brand','offer_id','file_id','load_ts','load_user','sitename','pre_post_flag','mcc','mnc','network_activity_id','event_date','productline_bonus_l4','filename','rec_id','retry_count','retry_ts','status_data','status','trx_lacci')

HASIL3=join6.join(product_lime,join6.service_filter ==  product_lime.rkey, how = 'left')\
    .withColumn('l4_name', when((F.col('l4_name') == "NQ") &(F.col('service_filter') == F.col('rkey')) & (F.col('service_type') == "3_") & (F.col('product_name').isNotNull()), (F.col('l4_name_1'))).otherwise("NQ"))\
    .withColumn('l3_name', when((F.col('l3_name') == "NQ") & (F.col('service_filter') == F.col('rkey')) & (F.col('service_type') == "3_") & (F.col('product_name').isNotNull()), (F.col('l3_name_1'))).otherwise("NQ"))\
    .withColumn('l2_name', when((F.col('l2_name') == "NQ") & (F.col('service_filter') == F.col('rkey')) & (F.col('service_type') == "3_") & (F.col('product_name').isNotNull()), (F.col('l2_name_1'))).otherwise("NQ"))\
    .withColumn('l1_name', when((F.col('l1_name') == "NQ") & (F.col('service_filter') == F.col('rkey')) & (F.col('service_type') == "3_") & (F.col('product_name').isNotNull()), (F.col('l3_name_1'))).otherwise("NQ"))\
    .select('timestamp_r','trx_date','trx_hour','a_party','account_owner','msisdn','subs_id','cust_type_desc','cust_subtype_desc','location_number_type_a','location_number_a','b_party','location_number_type_b','location_number_b','number_of_reroutings','imsi','number_of_network_reroutings','redirecting_party_id','pre_call_duration','call_duration','charging_id','roaming_flag','vlr_num','cell_id','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','negotiated_qos','requested_qos','subscribed_qos','dialled_apn','eventtype','providerid','currency','subscriber_status','forwarding_flag','first_call_flag','camel_roaming','time_zone','account_balance','account_number','event_source','guiding_resource_type','rounded_duration','total_volume','rounded_total_volume','event_start_period','cifa','account_delta','bonus_consumed','credit_indicator','rev','discount_amount','free_total_volume','free_duration','call_direction','charge_code_description','special_number_ind','bonus_information','internal_cause','basic_cause','time_band','call_type','vas_code','service_filter','national_calling_zone','national_called_zone','international_calling_zone','international_called_zone','event_id','access_code','country_name','cp_name','content_id','rating_group','bft_indicator','unapplied_amount','partition_id','rated_date_time','area','cost_band','l4_name','l3_name','l2_name','l1_name','rating_offer_id','settlement_indicator','tap_code','mcc_mnc','rating_pricing_item_id','file_identifier','record_number','future_string1','future_string2','future_string3','allowance_consumed_revenue','ifrs_ind_for_allowance_revenue','ifrs_ind_for_vas_event','itemized_bill_fee','consumed_monetary_allowance','partial_charge_ind','brand','offer_id','file_id','load_ts','load_user','sitename','pre_post_flag','mcc','mnc','network_activity_id','event_date','productline_bonus_l4','filename','rec_id','retry_count','retry_ts','status_data','status','trx_lacci')

HASIL4=service5.join(product_lime,service5.vas_code ==  product_lime.rkey, how = 'left')\
    .withColumn('l4_name', when((F.col('vas_code') == F.col('rkey')) & (F.col('service_type') == "5_"), (F.col('l4_name_1'))).otherwise("NQ"))\
    .withColumn('l3_name', when((F.col('vas_code') == F.col('rkey')) & (F.col('service_type') == "5_"), (F.col('l3_name_1'))).otherwise("NQ"))\
    .withColumn('l2_name', when((F.col('vas_code') == F.col('rkey')) & (F.col('service_type') == "5_"), (F.col('l2_name_1'))).otherwise("NQ"))\
    .withColumn('l1_name', when((F.col('vas_code') == F.col('rkey')) & (F.col('service_type') == "5_"), (F.col('l1_name_1'))).otherwise("NQ"))\
    .select('timestamp_r','trx_date','trx_hour','a_party','account_owner','msisdn','subs_id','cust_type_desc','cust_subtype_desc','location_number_type_a','location_number_a','b_party','location_number_type_b','location_number_b','number_of_reroutings','imsi','number_of_network_reroutings','redirecting_party_id','pre_call_duration','call_duration','charging_id','roaming_flag','vlr_num','cell_id','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','negotiated_qos','requested_qos','subscribed_qos','dialled_apn','eventtype','providerid','currency','subscriber_status','forwarding_flag','first_call_flag','camel_roaming','time_zone','account_balance','account_number','event_source','guiding_resource_type','rounded_duration','total_volume','rounded_total_volume','event_start_period','cifa','account_delta','bonus_consumed','credit_indicator','rev','discount_amount','free_total_volume','free_duration','call_direction','charge_code_description','special_number_ind','bonus_information','internal_cause','basic_cause','time_band','call_type','vas_code','service_filter','national_calling_zone','national_called_zone','international_calling_zone','international_called_zone','event_id','access_code','country_name','cp_name','content_id','rating_group','bft_indicator','unapplied_amount','partition_id','rated_date_time','area','cost_band','l4_name','l3_name','l2_name','l1_name','rating_offer_id','settlement_indicator','tap_code','mcc_mnc','rating_pricing_item_id','file_identifier','record_number','future_string1','future_string2','future_string3','allowance_consumed_revenue','ifrs_ind_for_allowance_revenue','ifrs_ind_for_vas_event','itemized_bill_fee','consumed_monetary_allowance','partial_charge_ind','brand','offer_id','file_id','load_ts','load_user','sitename','pre_post_flag','mcc','mnc','network_activity_id','event_date','productline_bonus_l4','filename','rec_id','retry_count','retry_ts','status_data','status','trx_lacci')

HASIL5=service_reject.select('timestamp_r','trx_date','trx_hour','a_party','account_owner','msisdn','subs_id','cust_type_desc','cust_subtype_desc','location_number_type_a','location_number_a','b_party','location_number_type_b','location_number_b','number_of_reroutings','imsi','number_of_network_reroutings','redirecting_party_id','pre_call_duration','call_duration','charging_id','roaming_flag','vlr_num','cell_id','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','negotiated_qos','requested_qos','subscribed_qos','dialled_apn','eventtype','providerid','currency','subscriber_status','forwarding_flag','first_call_flag','camel_roaming','time_zone','account_balance','account_number','event_source','guiding_resource_type','rounded_duration','total_volume','rounded_total_volume','event_start_period','cifa','account_delta','bonus_consumed','credit_indicator','rev','discount_amount','free_total_volume','free_duration','call_direction','charge_code_description','special_number_ind','bonus_information','internal_cause','basic_cause','time_band','call_type','vas_code','service_filter','national_calling_zone','national_called_zone','international_calling_zone','international_called_zone','event_id','access_code','country_name','cp_name','content_id','rating_group','bft_indicator','unapplied_amount','partition_id','rated_date_time','area','cost_band','l4_name','l3_name','l2_name','l1_name','rating_offer_id','settlement_indicator','tap_code','mcc_mnc','rating_pricing_item_id','file_identifier','record_number','future_string1','future_string2','future_string3','allowance_consumed_revenue','ifrs_ind_for_allowance_revenue','ifrs_ind_for_vas_event','itemized_bill_fee','consumed_monetary_allowance','partial_charge_ind','brand','offer_id','file_id','load_ts','load_user','sitename','pre_post_flag','mcc','mnc','network_activity_id','event_date','productline_bonus_l4','filename','rec_id','retry_count','retry_ts','status_data','status','trx_lacci')

from functools import reduce 
df = [ HASIL1,HASIL2,HASIL3,HASIL4,HASIL5]
MERGE = reduce(DataFrame.unionAll, df)\
    .withColumn('subs_offer', substring('offer_id', 3,8))

BONUS=hv.table('CHG_1.bonus')\
    .withColumn('subs_l4', trim('l4'))\
    .withColumn('subs_l4', substring('l4', 4,8))
join_bonus=MERGE.join(BONUS,MERGE.subs_offer ==  BONUS.subs_l4, how = 'left')\
    .withColumn('productline_bonus_l4', when((F.col('subs_offer') ==  F.col('subs_l4')), (F.col('l4'))).otherwise(""))\
    .withColumn('key_timestap_r', lit (F.col('timestamp_r')))\
    .withColumn('key_a_party', lit (F.col('a_party')))\
    .withColumn('key_b_party', when((F.col('service_filter').like('GPRS')), "").otherwise(F.col('b_party')))\
    .withColumn('key_call_duration', when((F.col('service_filter').like('GPRS')), "0").otherwise(F.col('call_duration')))\
    .withColumn('key_charging_id', when((F.col('service_filter').like('GPRS')), (F.col('charging_id'))).otherwise(""))\
    .withColumn('key_total_volume', when((F.col('service_filter').like('GPRS')), (F.col('total_volume'))).otherwise("0"))\
    .withColumn('key_account_delta', lit (F.col('account_delta')))\
    .withColumn('key_bonus_consumed', lit (F.col('bonus_consumed')))\
    .select('timestamp_r','trx_date','trx_hour','a_party','account_owner','msisdn','subs_id','cust_type_desc','cust_subtype_desc','location_number_type_a','location_number_a','b_party','location_number_type_b','location_number_b','number_of_reroutings','imsi','number_of_network_reroutings','redirecting_party_id','pre_call_duration','call_duration','charging_id','roaming_flag','vlr_num','cell_id','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','negotiated_qos','requested_qos','subscribed_qos','dialled_apn','eventtype','providerid','currency','subscriber_status','forwarding_flag','first_call_flag','camel_roaming','time_zone','account_balance','account_number','event_source','guiding_resource_type','rounded_duration','total_volume','rounded_total_volume','event_start_period','cifa','account_delta','bonus_consumed','credit_indicator','rev','discount_amount','free_total_volume','free_duration','call_direction','charge_code_description','special_number_ind','bonus_information','internal_cause','basic_cause','time_band','call_type','vas_code','service_filter','national_calling_zone','national_called_zone','international_calling_zone','international_called_zone','event_id','access_code','country_name','cp_name','content_id','rating_group','bft_indicator','unapplied_amount','partition_id','rated_date_time','area','cost_band','l4_name','l3_name','l2_name','l1_name','rating_offer_id','settlement_indicator','tap_code','mcc_mnc','rating_pricing_item_id','file_identifier','record_number','future_string1','future_string2','future_string3','allowance_consumed_revenue','ifrs_ind_for_allowance_revenue','ifrs_ind_for_vas_event','itemized_bill_fee','consumed_monetary_allowance','partial_charge_ind','brand','offer_id','file_id','load_ts','load_user','sitename','pre_post_flag','mcc','mnc','network_activity_id','event_date','productline_bonus_l4','filename','rec_id','retry_count','retry_ts','status_data','status','trx_lacci','key_timestap_r','key_a_party','key_b_party','key_call_duration','key_charging_id','key_total_volume','key_account_delta','key_bonus_consumed')

windowSpec  = Window.partitionBy('key_timestap_r','key_a_party','key_b_party','key_call_duration','key_charging_id','key_total_volume','key_account_delta','key_bonus_consumed')\
    .orderBy('filename','rec_id')
    
ranking=join_bonus.withColumn("rank",rank().over(windowSpec))\
    .withColumn('trim_subs_id', trim ('subs_id')).withColumn('trim_cust_type_desc', trim ('cust_type_desc'))\
    .withColumn('trim_cust_subtype_desc', trim ('cust_subtype_desc')).withColumn('trim_lacci_id', trim ('lacci_id'))\
    .withColumn('trim_lacci_closing_flag', trim ('lacci_closing_flag')).withColumn('trim_lac', trim ('lac')).withColumn('trim_ci', trim ('ci'))\
    .withColumn('trim_node_type', trim ('node_type')).withColumn('trim_area_sales', trim ('area_sales'))\
    .withColumn('trim_region_sales', trim ('region_sales')).withColumn('trim_branch', trim ('branch'))\
    .withColumn('trim_subbranch', trim ('subbranch')).withColumn('trim_cluster_sales', trim ('cluster_sales'))\
    .withColumn('trim_provinsi', trim ('provinsi')).withColumn('trim_kabupaten', trim ('kabupaten'))\
    .withColumn('trim_kecamatan', trim ('kecamatan')).withColumn('trim_kelurahan', trim ('kelurahan'))\
    .withColumn('trim_l4_name', trim ('l4_name')).withColumn('trim_l3_name', trim ('l3_name'))\
    .withColumn('trim_l2_name', trim ('l2_name')).withColumn('trim_l1_name',trim ('l1_name'))
    
#ranking=ranking.withColumn('hasil_rank', when((ranking.rank == "1"), "GOOD").otherwise("REJECT"))
NODUP=ranking.filter(col('rank') == 1)
DUP=ranking.filter(col('rank') != 1)

DUP=DUP.withColumn('status_data',when((DUP.subs_id.isNull()) | \
        (DUP.subs_id == ("-99")) | \
        (DUP.trim_subs_id.isNull()) | \
        (DUP.cust_type_desc.isNull()) | \
        (DUP.trim_cust_type_desc.isNull()) | \
        (DUP.cust_type_desc == ("NQ")) | \
        (DUP.cust_subtype_desc.isNull()) | \
        (DUP.trim_cust_subtype_desc.isNull()) | \
        (DUP.cust_subtype_desc == ("NQ")) | \
        (DUP.lacci_id.isNull()) | \
        (DUP.trim_lacci_id.isNull()) | \
        (DUP.lacci_id == ("-99")) | \
        (DUP.lacci_closing_flag.isNull()) | \
        (DUP.trim_lacci_closing_flag.isNull()) | \
        (DUP.lacci_closing_flag == ("-1")) | \
        (DUP.lac.isNull()) | \
        (DUP.trim_lac.isNull()) | \
        (DUP.lac == ("NQ")) | \
        (DUP.ci.isNull()) | \
        (DUP.trim_ci.isNull()) | \
        (DUP.ci == ("NQ")) | \
        (DUP.node_type.isNull()) | \
        (DUP.trim_node_type.isNull()) | \
        (DUP.node_type == ("NQ")) | \
        (DUP.area_sales.isNull()) | \
        (DUP.trim_area_sales.isNull()) | \
        (DUP.area_sales == ("NQ")) | \
        (DUP.region_sales.isNull()) | \
        (DUP.trim_region_sales.isNull()) | \
        (DUP.region_sales == ("NQ")) | \
        (DUP.branch.isNull()) | \
        (DUP.trim_branch.isNull()) | \
        (DUP.branch == ("NQ")) | \
        (DUP.subbranch.isNull()) | \
        (DUP.trim_subbranch.isNull()) | \
        (DUP.subbranch == ("NQ")) | \
        (DUP.cluster_sales.isNull()) | \
        (DUP.trim_cluster_sales.isNull()) | \
        (DUP.cluster_sales == ("NQ")) | \
        (DUP.provinsi.isNull()) | \
        (DUP.trim_provinsi.isNull()) | \
        (DUP.provinsi == ("NQ")) | \
        (DUP.kabupaten.isNull()) | \
        (DUP.trim_kabupaten.isNull()) | \
        (DUP.kabupaten == ("NQ")) | \
        (DUP.kecamatan.isNull()) | \
        (DUP.trim_kecamatan.isNull()) | \
        (DUP.kecamatan == ("NQ")) | \
        (DUP.kelurahan.isNull()) | \
        (DUP.trim_kelurahan.isNull()) | \
        (DUP.kelurahan == ("NQ")) | \
        (DUP.l4_name.isNull()) | \
        (DUP.trim_l4_name.isNull()) | \
        (DUP.l4_name == ("NQ")) | \
        (DUP.l3_name.isNull()) | \
        (DUP.trim_l3_name.isNull()) | \
        (DUP.l3_name == ("NQ")) | \
        (DUP.l2_name.isNull()) | \
        (DUP.trim_l2_name.isNull()) | \
        (DUP.l2_name == ("NQ")) | \
        (DUP.l1_name.isNull()) | \
        (DUP.trim_l1_name.isNull()) | \
        (DUP.l1_name == ("NQ")), "REJECT").otherwise("GOOD"))\
    .withColumn("status", lit("DUP"))

windowSpec2  = Window.partitionBy('filename','rec_id')\
    .orderBy('timestamp_r','trx_date','trx_hour','a_party','account_owner','msisdn','subs_id','cust_type_desc','cust_subtype_desc','location_number_type_a','location_number_a','b_party','location_number_type_b','location_number_b','number_of_reroutings','imsi','number_of_network_reroutings','redirecting_party_id','pre_call_duration','call_duration','charging_id','roaming_flag','vlr_num','cell_id','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','negotiated_qos','requested_qos','subscribed_qos','dialled_apn','eventtype','providerid','currency','subscriber_status','forwarding_flag','first_call_flag','camel_roaming','time_zone','account_balance','account_number','event_source','guiding_resource_type','rounded_duration','total_volume','rounded_total_volume','event_start_period','cifa','account_delta','bonus_consumed','credit_indicator','rev','discount_amount','free_total_volume','free_duration','call_direction','charge_code_description','special_number_ind','bonus_information','internal_cause','basic_cause','time_band','call_type','vas_code','service_filter','national_calling_zone','national_called_zone','international_calling_zone','international_called_zone','event_id','access_code','country_name','cp_name','content_id','rating_group','bft_indicator','unapplied_amount','partition_id','rated_date_time','area','cost_band','l4_name','l3_name','l2_name','l1_name','rating_offer_id','settlement_indicator','tap_code','mcc_mnc','rating_pricing_item_id','file_identifier','record_number','future_string1','future_string2','future_string3','allowance_consumed_revenue','ifrs_ind_for_allowance_revenue','ifrs_ind_for_vas_event','itemized_bill_fee','consumed_monetary_allowance','partial_charge_ind','brand','offer_id','file_id','load_ts','load_user','sitename','pre_post_flag','mcc','mnc','network_activity_id','event_date','productline_bonus_l4','filename','rec_id','retry_count','retry_ts','status_data','status')
DUP=DUP.withColumn("rangking",rank().over(windowSpec2))\
    .select('timestamp_r','trx_date','trx_hour','a_party','account_owner','msisdn','subs_id','cust_type_desc','cust_subtype_desc','location_number_type_a','location_number_a','b_party','location_number_type_b','location_number_b','number_of_reroutings','imsi','number_of_network_reroutings','redirecting_party_id','pre_call_duration','call_duration','charging_id','roaming_flag','vlr_num','cell_id','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','negotiated_qos','requested_qos','subscribed_qos','dialled_apn','eventtype','providerid','currency','subscriber_status','forwarding_flag','first_call_flag','camel_roaming','time_zone','account_balance','account_number','event_source','guiding_resource_type','rounded_duration','total_volume','rounded_total_volume','event_start_period','cifa','account_delta','bonus_consumed','credit_indicator','rev','discount_amount','free_total_volume','free_duration','call_direction','charge_code_description','special_number_ind','bonus_information','internal_cause','basic_cause','time_band','call_type','vas_code','service_filter','national_calling_zone','national_called_zone','international_calling_zone','international_called_zone','event_id','access_code','country_name','cp_name','content_id','rating_group','bft_indicator','unapplied_amount','partition_id','rated_date_time','area','cost_band','l4_name','l3_name','l2_name','l1_name','rating_offer_id','settlement_indicator','tap_code','mcc_mnc','rating_pricing_item_id','file_identifier','record_number','future_string1','future_string2','future_string3','allowance_consumed_revenue','ifrs_ind_for_allowance_revenue','ifrs_ind_for_vas_event','itemized_bill_fee','consumed_monetary_allowance','partial_charge_ind','brand','offer_id','file_id','load_ts','load_user','sitename','pre_post_flag','mcc','mnc','network_activity_id','event_date','productline_bonus_l4','filename','rec_id','retry_count','retry_ts','status_data','status','rangking')

NODUP=NODUP.withColumn('status_data',when((NODUP.subs_id.isNull()) | \
        (NODUP.subs_id == ("-99")) | \
        (NODUP.trim_subs_id.isNull()) | \
        (NODUP.cust_type_desc.isNull()) | \
        (NODUP.trim_cust_type_desc.isNull()) | \
        (NODUP.cust_type_desc == ("NQ")) | \
        (NODUP.cust_subtype_desc.isNull()) | \
        (NODUP.trim_cust_subtype_desc.isNull()) | \
        (NODUP.cust_subtype_desc == ("NQ")) | \
        (NODUP.lacci_id.isNull()) | \
        (NODUP.trim_lacci_id.isNull()) | \
        (NODUP.lacci_id == ("-99")) | \
        (NODUP.lacci_closing_flag.isNull()) | \
        (NODUP.trim_lacci_closing_flag.isNull()) | \
        (NODUP.lacci_closing_flag == ("-1")) | \
        (NODUP.lac.isNull()) | \
        (NODUP.trim_lac.isNull()) | \
        (NODUP.lac == ("NQ")) | \
        (NODUP.ci.isNull()) | \
        (NODUP.trim_ci.isNull()) | \
        (NODUP.ci == ("NQ")) | \
        (NODUP.node_type.isNull()) | \
        (NODUP.trim_node_type.isNull()) | \
        (NODUP.node_type == ("NQ")) | \
        (NODUP.area_sales.isNull()) | \
        (NODUP.trim_area_sales.isNull()) | \
        (NODUP.area_sales == ("NQ")) | \
        (NODUP.region_sales.isNull()) | \
        (NODUP.trim_region_sales.isNull()) | \
        (NODUP.region_sales == ("NQ")) | \
        (NODUP.branch.isNull()) | \
        (NODUP.trim_branch.isNull()) | \
        (NODUP.branch == ("NQ")) | \
        (NODUP.subbranch.isNull()) | \
        (NODUP.trim_subbranch.isNull()) | \
        (NODUP.subbranch == ("NQ")) | \
        (NODUP.cluster_sales.isNull()) | \
        (NODUP.trim_cluster_sales.isNull()) | \
        (NODUP.cluster_sales == ("NQ")) | \
        (NODUP.provinsi.isNull()) | \
        (NODUP.trim_provinsi.isNull()) | \
        (NODUP.provinsi == ("NQ")) | \
        (NODUP.kabupaten.isNull()) | \
        (NODUP.trim_kabupaten.isNull()) | \
        (NODUP.kabupaten == ("NQ")) | \
        (NODUP.kecamatan.isNull()) | \
        (NODUP.trim_kecamatan.isNull()) | \
        (NODUP.kecamatan == ("NQ")) | \
        (NODUP.kelurahan.isNull()) | \
        (NODUP.trim_kelurahan.isNull()) | \
        (NODUP.kelurahan == ("NQ")) | \
        (NODUP.l4_name.isNull()) | \
        (NODUP.trim_l4_name.isNull()) | \
        (NODUP.l4_name == ("NQ")) | \
        (NODUP.l3_name.isNull()) | \
        (NODUP.trim_l3_name.isNull()) | \
        (NODUP.l3_name == ("NQ")) | \
        (NODUP.l2_name.isNull()) | \
        (NODUP.trim_l2_name.isNull()) | \
        (NODUP.l2_name == ("NQ")) | \
        (NODUP.l1_name.isNull()) | \
        (NODUP.trim_l1_name.isNull()) | \
        (NODUP.l1_name == ("NQ")), "REJECT").otherwise("GOOD"))\
    .withColumn("status", when((NODUP.status == "DUP"), (NODUP.status)).otherwise("NODUP"))


SUM=NODUP.withColumn("periode", regexp_replace("productline_bonus_l4","-",""))\
    .withColumn("filename", lit(NODUP.filename))\
    .withColumn("job_id", lit("20230206150805"))\
    .withColumn("group_process", lit(1))\
    .withColumn("status_data", lit(NODUP.status_data))\
    .withColumn("status", lit(NODUP.status))\
    .withColumn("qty", lit(1))\
    .withColumn("rev",lit(1))\
    .withColumn("filetype",lit("SA_USAGE_OCS_CHG"))\
    .groupBy('periode','filename','job_id','group_process','status_data','status','filetype')\
    .agg(sum('qty').alias('qty'),sum('rev').alias('rev'))\
    .select('periode','filename','job_id','group_process','status_data','status','qty','rev','filetype')

OUT_GOOD=NODUP.filter(col('status_data') == "GOOD").select('timestamp_r','trx_date','trx_hour','a_party','account_owner','msisdn','subs_id','cust_type_desc','cust_subtype_desc','location_number_type_a','location_number_a','b_party','location_number_type_b','location_number_b','number_of_reroutings','imsi','number_of_network_reroutings','redirecting_party_id','pre_call_duration','call_duration','charging_id','roaming_flag','vlr_num','cell_id','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','negotiated_qos','requested_qos','subscribed_qos','dialled_apn','eventtype','providerid','currency','subscriber_status','forwarding_flag','first_call_flag','camel_roaming','time_zone','account_balance','account_number','event_source','guiding_resource_type','rounded_duration','total_volume','rounded_total_volume','event_start_period','cifa','account_delta','bonus_consumed','credit_indicator','rev','discount_amount','free_total_volume','free_duration','call_direction','charge_code_description','special_number_ind','bonus_information','internal_cause','basic_cause','time_band','call_type','vas_code','service_filter','national_calling_zone','national_called_zone','international_calling_zone','international_called_zone','event_id','access_code','country_name','cp_name','content_id','rating_group','bft_indicator','unapplied_amount','partition_id','rated_date_time','area','cost_band','l4_name','l3_name','l2_name','l1_name','rating_offer_id','settlement_indicator','tap_code','mcc_mnc','rating_pricing_item_id','file_identifier','record_number','future_string1','future_string2','future_string3','allowance_consumed_revenue','ifrs_ind_for_allowance_revenue','ifrs_ind_for_vas_event','itemized_bill_fee','consumed_monetary_allowance','partial_charge_ind','brand','offer_id','file_id','load_ts','load_user','sitename','pre_post_flag','mcc','mnc','network_activity_id','event_date','productline_bonus_l4','filename','rec_id','retry_count','retry_ts')
OUT_REJECT=NODUP.filter(col('status_data') == "REJECT").select('timestamp_r','trx_date','trx_hour','a_party','account_owner','msisdn','subs_id','cust_type_desc','cust_subtype_desc','location_number_type_a','location_number_a','b_party','location_number_type_b','location_number_b','number_of_reroutings','imsi','number_of_network_reroutings','redirecting_party_id','pre_call_duration','call_duration','charging_id','roaming_flag','vlr_num','cell_id','lacci_id','lacci_closing_flag','lac','ci','node_type','area_sales','region_sales','branch','subbranch','cluster_sales','provinsi','kabupaten','kecamatan','kelurahan','negotiated_qos','requested_qos','subscribed_qos','dialled_apn','eventtype','providerid','currency','subscriber_status','forwarding_flag','first_call_flag','camel_roaming','time_zone','account_balance','account_number','event_source','guiding_resource_type','rounded_duration','total_volume','rounded_total_volume','event_start_period','cifa','account_delta','bonus_consumed','credit_indicator','rev','discount_amount','free_total_volume','free_duration','call_direction','charge_code_description','special_number_ind','bonus_information','internal_cause','basic_cause','time_band','call_type','vas_code','service_filter','national_calling_zone','national_called_zone','international_calling_zone','international_called_zone','event_id','access_code','country_name','cp_name','content_id','rating_group','bft_indicator','unapplied_amount','partition_id','rated_date_time','area','cost_band','l4_name','l3_name','l2_name','l1_name','rating_offer_id','settlement_indicator','tap_code','mcc_mnc','rating_pricing_item_id','file_identifier','record_number','future_string1','future_string2','future_string3','allowance_consumed_revenue','ifrs_ind_for_allowance_revenue','ifrs_ind_for_vas_event','itemized_bill_fee','consumed_monetary_allowance','partial_charge_ind','brand','offer_id','file_id','load_ts','load_user','sitename','pre_post_flag','mcc','mnc','network_activity_id','event_date','productline_bonus_l4','filename','rec_id','retry_count','retry_ts')
DUP.show()

DUP.write.csv('file:///home/hdoop/spark-3.3.1-bin-hadoop3/USAGE_DUP',sep ='~')
SUM.write.csv('file:///home/hdoop/spark-3.3.1-bin-hadoop3/SUM_DUP',sep ='~')
OUT_GOOD.write.csv('file:///home/hdoop/spark-3.3.1-bin-hadoop3/USAGE_GOOD',sep ='~')
OUT_REJECT.write.csv('file:///home/hdoop/spark-3.3.1-bin-hadoop3/USAGE_REJECT',sep ='~')

####
hv.sql("use CHG_1")
hv.sql("CREATE TABLE IF NOT EXISTS CHG_DUP (timestamp_r string,trx_date string,trx_hour string,a_party string,account_owner string,msisdn string,subs_id string,cust_type_desc string,cust_subtype_desc string,location_number_type_a string,location_number_a string,b_party string,location_number_type_b string,location_number_b string,number_of_reroutings string,imsi string,number_of_network_reroutings string,redirecting_party_id string,pre_call_duration string,call_duration string,charging_id string,roaming_flag string,vlr_num string,cell_id string,lacci_id string,lacci_closing_flag string,lac string,ci string,node_type string,area_sales string,region_sales string,branch string,subbranch string,cluster_sales string,provinsi string,kabupaten string,kecamatan string,kelurahan string,negotiated_qos string,requested_qos string,subscribed_qos string,dialled_apn string,eventtype string,providerid string,currency string,subscriber_status string,forwarding_flag string,first_call_flag string,camel_roaming string,time_zone string,account_balance string,account_number string,event_source string,guiding_resource_type string,rounded_duration string,total_volume string,rounded_total_volume string,event_start_period string,cifa string,account_delta string,bonus_consumed string,credit_indicator string,rev string,discount_amount string,free_total_volume string,free_duration string,call_direction string,charge_code_description string,special_number_ind string,bonus_information string,internal_cause string,basic_cause string,time_band string,call_type string,vas_code string,service_filter string,national_calling_zone string,national_called_zone string,international_calling_zone string,international_called_zone string,event_id string,access_code string,country_name string,cp_name string,content_id string,rating_group string,bft_indicator string,unapplied_amount string,partition_id string,rated_date_time string,area string,cost_band string,l4_name string,l3_name string,l2_name string,l1_name string,rating_offer_id string,settlement_indicator string,tap_code string,mcc_mnc string,rating_pricing_item_id string,file_identifier string,record_number string,future_string1 string,future_string2 string,future_string3 string,allowance_consumed_revenue string,ifrs_ind_for_allowance_revenue string,ifrs_ind_for_vas_event string,itemized_bill_fee string,consumed_monetary_allowance string,partial_charge_ind string,brand string,offer_id string,file_id string,load_ts string,load_user string,sitename string,pre_post_flag string,mcc string,mnc string,network_activity_id string,event_date string,productline_bonus_l4 string,filename string,rec_id string,retry_count string,retry_ts string,status_data string,status string,rangking string)\
    row format delimited fields terminated by '~'\
    ")
hv.sql("load data local inpath 'file:///home/hdoop/spark-3.3.1-bin-hadoop3/USAGE_DUP' overwrite into table CHG_DUP")

hv.sql("CREATE TABLE IF NOT EXISTS sum_chg (periode string,filename string,job_id string,group_process string,status_data string,status string,qty string,rev string,filetype string)\
    row format delimited fields terminated by '~'\
    ")
hv.sql("load data local inpath 'file:///home/hdoop/spark-3.3.1-bin-hadoop3/SUM_DUP' overwrite into table sum_chg")

hv.sql("CREATE TABLE IF NOT EXISTS good_chg (timestamp_r string,trx_date string,trx_hour string,a_party string,account_owner string,msisdn string,subs_id string,cust_type_desc string,cust_subtype_desc string,location_number_type_a string,location_number_a string,b_party string,location_number_type_b string,location_number_b string,number_of_reroutings string,imsi string,number_of_network_reroutings string,redirecting_party_id string,pre_call_duration string,call_duration string,charging_id string,roaming_flag string,vlr_num string,cell_id string,lacci_id string,lacci_closing_flag string,lac string,ci string,node_type string,area_sales string,region_sales string,branch string,subbranch string,cluster_sales string,provinsi string,kabupaten string,kecamatan string,kelurahan string,negotiated_qos string,requested_qos string,subscribed_qos string,dialled_apn string,eventtype string,providerid string,currency string,subscriber_status string,forwarding_flag string,first_call_flag string,camel_roaming string,time_zone string,account_balance string,account_number string,event_source string,guiding_resource_type string,rounded_duration string,total_volume string,rounded_total_volume string,event_start_period string,cifa string,account_delta string,bonus_consumed string,credit_indicator string,rev string,discount_amount string,free_total_volume string,free_duration string,call_direction string,charge_code_description string,special_number_ind string,bonus_information string,internal_cause string,basic_cause string,time_band string,call_type string,vas_code string,service_filter string,national_calling_zone string,national_called_zone string,international_calling_zone string,international_called_zone string,event_id string,access_code string,country_name string,cp_name string,content_id string,rating_group string,bft_indicator string,unapplied_amount string,partition_id string,rated_date_time string,area string,cost_band string,l4_name string,l3_name string,l2_name string,l1_name string,rating_offer_id string,settlement_indicator string,tap_code string,mcc_mnc string,rating_pricing_item_id string,file_identifier string,record_number string,future_string1 string,future_string2 string,future_string3 string,allowance_consumed_revenue string,ifrs_ind_for_allowance_revenue string,ifrs_ind_for_vas_event string,itemized_bill_fee string,consumed_monetary_allowance string,partial_charge_ind string,brand string,offer_id string,file_id string,load_ts string,load_user string,sitename string,pre_post_flag string,mcc string,mnc string,network_activity_id string,event_date string,productline_bonus_l4 string,filename string,rec_id string,retry_count string,retry_ts string)\
    row format delimited fields terminated by '~'\
    ")
hv.sql("load data local inpath 'file:///home/hdoop/spark-3.3.1-bin-hadoop3/USAGE_GOOD' overwrite into table good_chg")

hv.sql("CREATE TABLE IF NOT EXISTS reject_chg (timestamp_r string,trx_date string,trx_hour string,a_party string,account_owner string,msisdn string,subs_id string,cust_type_desc string,cust_subtype_desc string,location_number_type_a string,location_number_a string,b_party string,location_number_type_b string,location_number_b string,number_of_reroutings string,imsi string,number_of_network_reroutings string,redirecting_party_id string,pre_call_duration string,call_duration string,charging_id string,roaming_flag string,vlr_num string,cell_id string,lacci_id string,lacci_closing_flag string,lac string,ci string,node_type string,area_sales string,region_sales string,branch string,subbranch string,cluster_sales string,provinsi string,kabupaten string,kecamatan string,kelurahan string,negotiated_qos string,requested_qos string,subscribed_qos string,dialled_apn string,eventtype string,providerid string,currency string,subscriber_status string,forwarding_flag string,first_call_flag string,camel_roaming string,time_zone string,account_balance string,account_number string,event_source string,guiding_resource_type string,rounded_duration string,total_volume string,rounded_total_volume string,event_start_period string,cifa string,account_delta string,bonus_consumed string,credit_indicator string,rev string,discount_amount string,free_total_volume string,free_duration string,call_direction string,charge_code_description string,special_number_ind string,bonus_information string,internal_cause string,basic_cause string,time_band string,call_type string,vas_code string,service_filter string,national_calling_zone string,national_called_zone string,international_calling_zone string,international_called_zone string,event_id string,access_code string,country_name string,cp_name string,content_id string,rating_group string,bft_indicator string,unapplied_amount string,partition_id string,rated_date_time string,area string,cost_band string,l4_name string,l3_name string,l2_name string,l1_name string,rating_offer_id string,settlement_indicator string,tap_code string,mcc_mnc string,rating_pricing_item_id string,file_identifier string,record_number string,future_string1 string,future_string2 string,future_string3 string,allowance_consumed_revenue string,ifrs_ind_for_allowance_revenue string,ifrs_ind_for_vas_event string,itemized_bill_fee string,consumed_monetary_allowance string,partial_charge_ind string,brand string,offer_id string,file_id string,load_ts string,load_user string,sitename string,pre_post_flag string,mcc string,mnc string,network_activity_id string,event_date string,productline_bonus_l4 string,filename string,rec_id string,retry_count string,retry_ts string)\
    row format delimited fields terminated by '~'\
    ")
hv.sql("load data local inpath 'file:///home/hdoop/spark-3.3.1-bin-hadoop3/USAGE_REJECT' overwrite into table reject_chg")
end_date = datetime.now() + timedelta(hours=7)
duration = (end_date - start_date).seconds
###
print(start_date.strftime('%Y-%m-%d %H:%M:%S'))
print(end_date.strftime('%Y-%m-%d %H:%M:%S'))
print(duration)

