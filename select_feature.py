import sys
import logging
from pyspark import SparkContext

DEVICE_ID_CLICK = "device_id_click"
DEVICE_ID_IMPRESSION = "device_id_impression"
DEVICE_IP_CLICK = "device_ip_click"
DEVICE_IP_IMPRESSION = "device_ip_impression"
AD_ID_CLICK = "ad_id_click"
AD_ID_IMPRESSION = "ad_id_impression"
QUERY_CAMPAIGN_ID_CLICK = "query_campaign_id_click"
QUERY_CAMPAIGN_ID_IMPRESSION = "query_campaign_id_impression"
QUERY_AD_ID_CLICK = "query_ad_id_click"
QUERY_AD_ID_IMPRESSION = "query_ad_id_impression"


def process_query(query):
    fields = query.split(" ")
    output = "_".join(fields)
    return output


def select_feature(output_dir, logger):
    sc = SparkContext(appName="CTR_Features")
    output_dir = output_dir
    data = sc.textFile(output_dir).map(lambda line: line.encode("utf8", "ignore").split(','))
    # count feature
    device_ip_click = data.map(lambda fields: (fields[0], int(fields[7]))).reduceByKey(lambda v1, v2: v1 + v2)
    device_ip_impression = data.map(lambda fields: (fields[0], 1)).reduceByKey(lambda v1, v2: v1 + v2)

    device_id_click = data.map(lambda fields: (fields[1], int(fields[7]))).reduceByKey(lambda v1, v2: v1 + v2)
    device_id_impression = data.map(lambda fields: (fields[1], 1)).reduceByKey(lambda v1, v2: v1 + v2)

    ad_id_click = data.map(lambda fields: (fields[4], int(fields[7]))).reduceByKey(lambda v1, v2: v1 + v2)
    ad_id_impression = data.map(lambda fields: (fields[4], 1)).reduceByKey(lambda v1, v2: v1 + v2)

    query_campaign_id_click = data.map(
        lambda fields: (process_query(fields[3]) + "_" + fields[5], int(fields[7]))).reduceByKey(lambda v1, v2: v1 + v2)
    query_campaign_id_impression = data.map(lambda fields: (process_query(fields[3]) + "_" + fields[5], 1)).reduceByKey(
        lambda v1, v2: v1 + v2)

    query_ad_id_click = data.map(
        lambda fields: (process_query(fields[3]) + "_" + fields[4], int(fields[7]))).reduceByKey(lambda v1, v2: v1 + v2)
    query_ad_id_impression = data.map(lambda fields: (process_query(fields[3]) + "_" + fields[4], 1)).reduceByKey(
        lambda v1, v2: v1 + v2)

    device_id_click.saveAsTextFile(output_dir + DEVICE_ID_CLICK)
    device_id_impression.saveAsTextFile(output_dir + DEVICE_ID_IMPRESSION)

    device_ip_click.saveAsTextFile(output_dir + DEVICE_IP_CLICK)
    device_ip_impression.saveAsTextFile(output_dir + DEVICE_IP_IMPRESSION)

    ad_id_click.saveAsTextFile(output_dir + AD_ID_CLICK)
    ad_id_impression.saveAsTextFile(output_dir + AD_ID_IMPRESSION)

    query_campaign_id_click.saveAsTextFile(output_dir + QUERY_CAMPAIGN_ID_CLICK)
    query_campaign_id_impression.saveAsTextFile(output_dir + QUERY_CAMPAIGN_ID_IMPRESSION)

    query_ad_id_click.saveAsTextFile(output_dir + QUERY_AD_ID_CLICK)
    query_ad_id_impression.saveAsTextFile(output_dir + QUERY_AD_ID_IMPRESSION)
    sc.stop()

    logger.info("Feature selection finished")


# Device IP, Device id,Session id,Query,AdId,CampaignId,Ad_category_Query_category(0/1),clicked(0/1)

if __name__ == "__main__":
    output_dir = sys.argv[1]  # raw search log

    logger = logging.getLogger()
    select_feature(output_dir, logger)
