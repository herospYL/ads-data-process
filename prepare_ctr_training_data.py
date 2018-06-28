import sys
import redis
import logging

from pyspark import SparkContext

from generate_click_log import CLICK_LOG_FILE
from select_feature import process_query
from store_feature import (DEVICE_ID_CLICK_PREFIX, DEVICE_ID_IMPRESSION_PREFIX, DEVICE_IP_CLICK_PREFIX,
                           DEVICE_IP_IMPRESSION_PREFIX, AD_ID_CLICK_PREFIX, AD_ID_IMPRESSION_PREFIX,
                           QUERY_CAMPAIGN_ID_CLICK_PREFIX, QUERY_CAMPAIGN_ID_IMPRESSION_PREFIX,
                           QUERY_AD_ID_CLICK_PREFIX, QUERY_AD_ID_IMPRESSION_PREFIX)

CTR_TRAINING_DATA = "ctr_training_data"

# Values are all string, default to always decode with utf-8
# Global variable, thread-safe, this is used for avoiding the issue that Python is not able to pickle thread.lock object
redis_client = redis.StrictRedis(decode_responses=True)

# Device IP, Device id,Session id,Query,AdId,CampaignId,Ad_category_Query_category(0/1),clicked(0/1)
def prepare_feature_val(fields):
    device_ip = fields[0]
    device_id = fields[1]
    query = process_query(fields[3])
    ad_id = fields[4]
    camp_id = fields[5]
    query_ad_category_match = fields[6]

    if query_ad_category_match == '1':
        query_ad_category_match = '1000000'
    else:
        query_ad_category_match = '0'

    device_id_click_key = DEVICE_ID_CLICK_PREFIX + "_" + device_id
    device_id_click_val = redis_client.get(device_id_click_key)
    if not device_id_click_val:
        device_id_click_val = "0"

    device_id_impression_key = DEVICE_ID_IMPRESSION_PREFIX + "_" + device_id
    device_id_impression_val = redis_client.get(device_id_impression_key)
    if not device_id_impression_val:
        device_id_impression_val = "0"

    device_ip_click_key = DEVICE_IP_CLICK_PREFIX + "_" + device_ip
    device_ip_click_val = redis_client.get(device_ip_click_key)
    # logger.debug("key={0}".format(device_ip_click_key))
    # logger.debug("val={0}".format(device_ip_click_val))
    if not device_ip_click_val:
        device_ip_click_val = "0"

    device_ip_impression_key = DEVICE_IP_IMPRESSION_PREFIX + "_" + device_ip
    device_ip_impression_val = redis_client.get(device_ip_impression_key)
    if not device_ip_impression_val:
        device_ip_impression_val = "0"

    ad_id_click_key = AD_ID_CLICK_PREFIX + "_" + ad_id
    ad_id_click_val = redis_client.get(ad_id_click_key)
    if not ad_id_click_val:
        ad_id_click_val = "0"

    ad_id_impression_key = AD_ID_IMPRESSION_PREFIX + "_" + ad_id
    ad_id_impression_val = redis_client.get(ad_id_impression_key)
    if not ad_id_impression_val:
        ad_id_impression_val = "0"

    query_campaign_id_click_key = QUERY_CAMPAIGN_ID_CLICK_PREFIX + "_" + query + "_" + camp_id
    query_campaign_id_click_val = redis_client.get(query_campaign_id_click_key)
    if not query_campaign_id_click_val:
        query_campaign_id_click_val = "0"

    query_campaign_id_impression_key = QUERY_CAMPAIGN_ID_IMPRESSION_PREFIX + "_" + query + "_" + camp_id
    query_campaign_id_impression_val = redis_client.get(query_campaign_id_impression_key)
    if not query_campaign_id_impression_val:
        query_campaign_id_impression_val = "0"

    query_ad_id_click_key = QUERY_AD_ID_CLICK_PREFIX + "_" + query + "_" + ad_id
    query_ad_id_click_val = redis_client.get(query_ad_id_click_key)
    if not query_ad_id_click_val:
        query_ad_id_click_val = "0"

    query_ad_id_impression_key = QUERY_AD_ID_IMPRESSION_PREFIX + "_" + query + "_" + ad_id
    query_ad_id_impression_val = redis_client.get(query_ad_id_impression_key)
    if not query_ad_id_impression_val:
        query_ad_id_impression_val = "0"

    features = [str(device_ip_click_val), str(device_ip_impression_val), str(device_id_click_val),
                str(device_id_impression_val), str(ad_id_click_val), str(ad_id_impression_val),
                str(query_campaign_id_click_val), str(query_campaign_id_impression_val), str(query_ad_id_click_val),
                str(query_ad_id_impression_val), query_ad_category_match]

    line = ",".join(features)
    # print line
    return line


def prepare_ctr_training_data(file_dir):
    # client = redis.StrictRedis(decode_responses=True)  # Values are all string, default to always decode with utf-8

    sc = SparkContext(appName="CTR_Features")

    click_log_file = file_dir + CLICK_LOG_FILE
    data = sc.textFile(click_log_file, 100).map(lambda line: line.split(','))  # File is large, need more partition
    feature_data = data.map(lambda fields: (prepare_feature_val(fields), int(fields[7])))

    ctr_training_data = file_dir + CTR_TRAINING_DATA
    feature_data.saveAsTextFile(ctr_training_data)
    sc.stop()

    logger = logging.getLogger()
    logger.info("CTR training data preparation finished")


if __name__ == "__main__":
    file_dir = sys.argv[1]

    prepare_ctr_training_data(file_dir)
