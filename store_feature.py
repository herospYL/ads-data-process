import glob
import sys
import json

import redis
import logging

from select_feature import (DEVICE_ID_CLICK, DEVICE_ID_IMPRESSION, DEVICE_IP_CLICK, DEVICE_IP_IMPRESSION, AD_ID_CLICK,
                            AD_ID_IMPRESSION, QUERY_CAMPAIGN_ID_CLICK, QUERY_CAMPAIGN_ID_IMPRESSION,
                            QUERY_AD_ID_CLICK, QUERY_AD_ID_IMPRESSION)

DEVICE_ID_CLICK_PREFIX = 'didc'
DEVICE_ID_IMPRESSION_PREFIX = 'didi'
DEVICE_IP_CLICK_PREFIX = 'dipc'
DEVICE_IP_IMPRESSION_PREFIX = 'dipi'
AD_ID_CLICK_PREFIX = 'aidc'
AD_ID_IMPRESSION_PREFIX = 'aidi'
QUERY_CAMPAIGN_ID_CLICK_PREFIX = 'qcidc'
QUERY_CAMPAIGN_ID_IMPRESSION_PREFIX = 'qcidi'
QUERY_AD_ID_CLICK_PREFIX = 'qaidc'
QUERY_AD_ID_IMPRESSION_PREFIX = 'qaidi'

FEATURE_STORE_FILE = "feature_store_file.txt"


def _write_feature(feature_dir, key_prefix, redis_client, output, logger):
    path = feature_dir + "/part*"

    for filename in glob.glob(path):
        logger.info("input data file:{0}".format(filename))
        with open(filename, 'r') as f:
            for line in f:
                line = line.strip().strip("()")
                # print "input line:",line
                fields = line.split(",")
                key = key_prefix + "_" + fields[0].strip("''")
                # print key
                val = fields[1]
                # print val
                redis_client.set(key, val)

                entry = {"feature_key": key, "feature_value": val}
                output.write(json.dumps(entry))  # json dump could have unordered result, which is okay
                output.write('\n')


def store_feature(file_dir):
    device_id_click = file_dir + DEVICE_ID_CLICK
    device_id_impression = file_dir + DEVICE_ID_IMPRESSION

    device_ip_click = file_dir + DEVICE_IP_CLICK
    device_ip_impression = file_dir + DEVICE_IP_IMPRESSION

    ad_id_click = file_dir + AD_ID_CLICK
    ad_id_impression = file_dir + AD_ID_IMPRESSION

    query_campaign_id_click = file_dir + QUERY_CAMPAIGN_ID_CLICK
    query_campaign_id_impression = file_dir + QUERY_CAMPAIGN_ID_IMPRESSION

    query_ad_id_click = file_dir + QUERY_AD_ID_CLICK
    query_ad_id_impression = file_dir + QUERY_AD_ID_IMPRESSION

    # Output targets initialization
    feature_store_file = file_dir + FEATURE_STORE_FILE
    output = open(feature_store_file, "a")
    client = redis.StrictRedis()
    logger = logging.getLogger()

    _write_feature(device_id_click, DEVICE_ID_CLICK_PREFIX, client, output, logger)
    _write_feature(device_id_impression, DEVICE_ID_IMPRESSION_PREFIX, client, output, logger)

    _write_feature(device_ip_click, DEVICE_IP_CLICK_PREFIX, client, output, logger)
    _write_feature(device_ip_impression, DEVICE_IP_IMPRESSION_PREFIX, client, output, logger)

    _write_feature(ad_id_click, AD_ID_CLICK_PREFIX, client, output, logger)
    _write_feature(ad_id_impression, AD_ID_IMPRESSION_PREFIX, client, output, logger)

    _write_feature(query_campaign_id_click, QUERY_CAMPAIGN_ID_CLICK_PREFIX, client, output, logger)
    _write_feature(query_campaign_id_impression, QUERY_CAMPAIGN_ID_IMPRESSION_PREFIX, client, output, logger)

    _write_feature(query_ad_id_click, QUERY_AD_ID_CLICK_PREFIX, client, output, logger)
    _write_feature(query_ad_id_impression, QUERY_AD_ID_IMPRESSION_PREFIX, client, output, logger)

    output.close()
    logger.info("Feature storage finished")


if __name__ == "__main__":
    file_dir = sys.argv[1]

    store_feature(file_dir)
