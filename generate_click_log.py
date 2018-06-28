import random
import sys
import time
import json
import logging

from generate_query_ad import (AD_FILE, QUERY_CAMP_AD_FILE, CAMPAIGN_WEIGHT_FILE, AD_WEIGHT_FILE,
                               CAMPAIGN_ID_CATEGORY_FILE, CAMPAIGN_ID_AD_ID_FILE)

USER_FILE = "user_file.txt"
CLICK_LOG_FILE = "click_log_file.txt"


# items is dict , key is the data, cal is weight
def _weighted_sampling(items):
    u = random.uniform(0, 1)
    # print "u",u
    cumulative_weight = 0.0
    while 1:
        for key in items:
            cumulative_weight += items[key]
            if u <= cumulative_weight:
                return key


def _test_weighted_sampling(query_camp_ad, campaign_weight):
    logger = logging.getLogger()
    for query_group_id in query_camp_ad:
        sample_camps_weight = {}
        sample_camps = {}
        for camp_id in query_camp_ad[query_group_id]:
            logger.debug("camp_id:{0}, weight:{1}".format(camp_id, campaign_weight[camp_id]))
            sample_camps_weight[camp_id] = campaign_weight[camp_id]

        total_freq = 0
        for i in range(1000):
            sample_camp_id = _weighted_sampling(sample_camps_weight)
            total_freq += 1
            if sample_camp_id in sample_camps:
                sample_camps[sample_camp_id] += 1
            else:
                sample_camps[sample_camp_id] = 1

        for sample_camp_id in sample_camps:
            logger.debug("camp_id:{0}, freq distribution:{1}".format(sample_camp_id, sample_camps[
                sample_camp_id] * 1.0 / total_freq))

            logger.debug("=================================")


def _random_campaign_id(exclusive_campaign_id):
    millis = int(round(time.time() * 1000))
    random.seed(millis)
    size_exclusive_camp_id = len(exclusive_campaign_id)
    index = 0
    if size_exclusive_camp_id > 1:
        index = random.randint(0, size_exclusive_camp_id - 1)
    sample_camp_id = exclusive_campaign_id[index]
    return sample_camp_id


def _random_ad_id(exclusive_ad_id):
    size_campaign_id_ad_id = len(exclusive_ad_id)
    index_ad = 0
    if size_campaign_id_ad_id > 1:
        index_ad = random.randint(0, size_campaign_id_ad_id - 1)
    sample_ad_id = exclusive_ad_id[index_ad]
    return sample_ad_id


def _mismatch_query_categr_ads_category_sampling(query_group_id, query_category, query_camp_ad,
                                                 campaign_id_category,
                                                 campaign_id_ad_id):
    exclusive_campaign_id = []
    for campaign_id in campaign_id_category:
        if query_category != campaign_id_category[campaign_id]:
            exclusive_campaign_id.append(campaign_id)

    for camp_id in exclusive_campaign_id:
        if camp_id in query_camp_ad[query_group_id]:
            exclusive_campaign_id.remove(camp_id)

    if len(exclusive_campaign_id) == 0:
        return ()
    sample_camp_id = _random_campaign_id(exclusive_campaign_id)
    sample_ad_id = _random_ad_id(campaign_id_ad_id[sample_camp_id])

    return sample_ad_id, sample_camp_id, 0


def _mismatched_query_campaign_id_ad_id_sampling(query_group_id, camp_id, query_camp_ad, campaign_id_ad_id):
    exclusive_campaign_id = []
    millis = int(round(time.time() * 1000))
    random.seed(millis)
    r = random.randint(0, 1)
    query_camp_category_match = 1
    if r == 0:
        # cross camp per query_group_id
        for campaign_id in query_camp_ad[query_group_id]:
            if campaign_id != camp_id:
                exclusive_campaign_id.append(campaign_id)
    else:
        # cross query group id
        query_camp_category_match = 0
        for q_g_id in query_camp_ad:
            if q_g_id != query_group_id:
                for campaign_id in query_camp_ad[q_g_id]:
                    exclusive_campaign_id.append(campaign_id)

    # print exclusive_campaign_id
    if len(exclusive_campaign_id) == 0:
        return ()
    sample_camp_id = _random_campaign_id(exclusive_campaign_id)
    sample_ad_id = _random_ad_id(campaign_id_ad_id[sample_camp_id])
    return sample_ad_id, sample_camp_id, query_camp_category_match


def _lowest_campaign_id_ad_id_weight(query_group_id, query_camp_ad, campaign_weight, ad_weight, campaign_id_ad_id):
    lowest_camp_weight = 1.0
    lowest_weight_camp_id = 0
    if len(query_camp_ad[query_group_id]) <= 1:
        return ()
    for campId in query_camp_ad[query_group_id]:
        w = campaign_weight[campId]
        if w < lowest_camp_weight:
            lowest_camp_weight = w
            lowest_weight_camp_id = campId

    lowest_weight = 1.0
    lowest_weight_ad_id = 0
    if len(campaign_id_ad_id[lowest_weight_camp_id]) <= 1:
        return ()
    for adId in campaign_id_ad_id[lowest_weight_camp_id]:
        w = ad_weight[str(adId)]
        if w < lowest_weight:
            lowest_weight = w
            lowest_weight_ad_id = adId

    return lowest_weight_ad_id, lowest_weight_camp_id, 1


# negative sample (no click query)type
# 0: mismatched query_categr ads_category 20%
# 1: mismatched Query_CampaignId 10%
# 2: lowest campaignId weight, lowest adId weight 30%
# 3: matched but no click 40%
# return AdId,CampaignId,Ad_category_Query_category(0/1)
def _negative_sampling(ip, device_id, ad, query_camp_ad, campaign_weight, ad_weight, campaign_id_category,
                       campaign_id_ad_id, negative_type):
    logger = logging.getLogger()
    fields = []
    query = ad["query"].lower()
    query_group_id = str(ad["query_group_id"])
    query_category = ad["category"].lower()
    camp_id = str(ad["campaignId"])
    ad_id = str(ad["adId"])
    result = (0, 0, 0)
    if len(campaign_id_ad_id[camp_id]) <= 1:
        return fields

    if negative_type == 0:
        result = _mismatch_query_categr_ads_category_sampling(query_group_id, query_category, query_camp_ad,
                                                              campaign_id_category, campaign_id_ad_id)

    if negative_type == 1:
        result = _mismatched_query_campaign_id_ad_id_sampling(query_group_id, camp_id, query_camp_ad,
                                                              campaign_id_ad_id)

    if negative_type == 2:
        result = _lowest_campaign_id_ad_id_weight(query_group_id, query_camp_ad, campaign_weight, ad_weight,
                                                  campaign_id_ad_id)

    if negative_type == 3:
        result = (ad_id, camp_id, 1, 0)

    if len(result) == 0:
        return fields

    if query == "" or str(result[0]) == "" or str(result[1]) == "" or str(result[2]) == "":
        logger.error(
            "invalid fields in negative_sampling {0} {1} {2} {3}".format(query, str(result[0]), str(result[1]),
                                                                         str(result[2])))
        return fields
    session_id = int(round(time.time() * 1000))
    # Device IP, Device id,Session id,Query,AdId,CampaignId,Ad_category_Query_category(0/1),clicked(0/1)

    fields.append(str(ip))
    fields.append(str(device_id))
    fields.append(str(session_id))
    fields.append(query)
    fields.append(str(result[0]))
    fields.append(str(result[1]))
    fields.append(str(result[2]))
    fields.append("0")
    return fields


def _all_positive_sampling(ip, device_id, query_group_id, ad_id_query, query_camp_ad, click_log_output):
    for camp_id in query_camp_ad[query_group_id]:
        for ad_id in query_camp_ad[query_group_id][camp_id]:
            query = ad_id_query[ad_id]
            session_id = int(round(time.time() * 1000))
            # Device IP, Device id,Session id,Query,AdId,CampaignId,clicked(0/1),Ad_category_Query_category(0/1)
            fields = [str(ip), str(device_id), str(session_id), query, str(ad_id), str(camp_id), "1", "1"]
            line = ",".join(fields)
            click_log_output.write(line)
            click_log_output.write('\n')


def _positive_sampling(ip, device_id, query_group_id, ad_id_query, query_camp_ad, campaign_weight, ad_weight):
    logger = logging.getLogger()
    cur_camp_weight = {}
    cur_ad_weight = {}
    # print "current query_group_id",query_group_id
    for camp_id in query_camp_ad[query_group_id]:
        cur_camp_weight[camp_id] = campaign_weight[camp_id]

    sample_camp_id = _weighted_sampling(cur_camp_weight)
    # print "current sample_camp_id",sample_camp_id

    for ad_id in query_camp_ad[query_group_id][sample_camp_id]:
        cur_ad_weight[ad_id] = ad_weight[str(ad_id)]

    sample_ad_id = _weighted_sampling(cur_ad_weight)
    # print "current sample_ad_id",sample_ad_id

    query = ad_id_query[sample_ad_id]

    session_id = int(round(time.time() * 1000))
    # Device IP, Device id,Session id,Query,AdId,CampaignId,Ad_category_Query_category(0/1),clicked(0/1)
    fields = []

    if query == "" or str(sample_ad_id) == "" or str(sample_camp_id) == "":
        logger.error(
            "invalid fields in positive_sampling {0} {1} {2}".format(query, str(sample_ad_id), str(sample_camp_id)))
        return fields

    fields.append(str(ip))
    fields.append(str(device_id))
    fields.append(str(session_id))
    fields.append(query)
    fields.append(str(sample_ad_id))
    fields.append(str(sample_camp_id))
    fields.append("1")
    fields.append("1")
    return fields


def _valid(fields):
    if len(fields) < 8:
        # print "invalid fields",fields
        return False

    return True


def generate_click_log(file_dir):
    ad_input_file = file_dir + AD_FILE
    user_input_file = file_dir + USER_FILE
    query_camp_ad_file = file_dir + QUERY_CAMP_AD_FILE
    campaign_weight_file = file_dir + CAMPAIGN_WEIGHT_FILE
    ad_weight_file = file_dir + AD_WEIGHT_FILE
    campaign_id_category_file = file_dir + CAMPAIGN_ID_CATEGORY_FILE
    campaign_id_ad_id_file = file_dir + CAMPAIGN_ID_AD_ID_FILE
    click_log_output_file = file_dir + CLICK_LOG_FILE

    ad_list = []
    ad_id_query = {}
    query_camp_ad = {}
    campaign_weight = {}
    ad_weight = {}
    campaign_id_category = {}
    campaign_id_ad_id = {}

    with open(ad_input_file, "r") as lines:
        for line in lines:
            entry = json.loads(line.strip())
            ad_list.append(entry)
            ad_id_query[entry["adId"]] = entry["query"].lower()

    with open(query_camp_ad_file) as json_data:
        query_camp_ad = json.load(json_data)
        # print query_camp_ad["1"]

    with open(campaign_weight_file) as json_data:
        campaign_weight = json.load(json_data)
        # print campaign_weight["8001"]

    with open(ad_weight_file) as json_data:
        ad_weight = json.load(json_data)
        # print ad_weight["1169"]

    # with open(query_group_id_query_file) as json_data:
    #    query_group_id_query = json.load(json_data)

    with open(campaign_id_category_file) as json_data:
        campaign_id_category = json.load(json_data)

    with open(campaign_id_ad_id_file) as json_data:
        campaign_id_ad_id = json.load(json_data)
    # test_weighted_sampling(query_camp_ad, campaign_weight)

    # split user to 4 level
    # level 0: 5% click for each query
    # level 1: 25% 1st 2 device id click for each query, rest 3 device_id click on 70% of query group, rest of 30% query  group no click
    # level 2: 30%  random  select 1 device_id click for 50% of query group
    # level 3: 40% never click

    num_ip = sum(1 for line in open(user_input_file))
    level_0_max_index = int(num_ip * 0.05)
    level_1_max_index = int(num_ip * 0.3)
    level_2_max_index = int(num_ip * 0.6)

    level_0_user = {}
    level_1_user = {}
    level_2_user = {}
    level_3_user = {}

    i = 0
    with open(user_input_file, "r") as lines:
        for line in lines:
            line = line.strip().strip("\n")
            fields = line.split(",")
            ip = fields[0]
            if i <= level_0_max_index:
                level_0_user[ip] = fields[1:6]
            if level_0_max_index < i <= level_1_max_index:
                level_1_user[ip] = fields[1:6]
            if level_1_max_index < i <= level_2_max_index:
                level_2_user[ip] = fields[1:6]
            if i > level_2_max_index:
                level_3_user[ip] = fields[1:6]
            i += 1

    click_log_output = open(click_log_output_file, "w")

    # negative sample (no click query)type

    # 0: mismatched query_categr ads_category 20%
    # 1: mismatched Query_CampaignId 10%
    # 2: lowest campaignId weight, lowest adId weight 30%
    # 3: matched but no click 40%
    num_ad_list = len(ad_list)
    negative_types = {0: 0.2, 1: 0.1, 2: 0.3, 3: 0.4}

    for ip in level_0_user:
        for device_id in level_0_user[ip]:
            for query_group_id in query_camp_ad:
                for i in range(1, 50):
                    fields = _positive_sampling(ip, device_id, query_group_id, ad_id_query, query_camp_ad,
                                                campaign_weight, ad_weight)
                    if not _valid(fields):
                        continue
                    line = ",".join(fields)
                    click_log_output.write(line)
                    click_log_output.write('\n')

                for i in range(1, 10):
                    _all_positive_sampling(ip, device_id, query_group_id, ad_id_query, query_camp_ad,
                                           click_log_output)

    for ip in level_1_user:
        # positive sample
        for x in range(2):
            device_id = level_1_user[ip][x]
            for query_group_id in query_camp_ad:
                for i in range(1, 50):
                    fields = _positive_sampling(ip, device_id, query_group_id, ad_id_query, query_camp_ad,
                                                campaign_weight, ad_weight)
                    if not _valid(fields):
                        continue
                    line = ",".join(fields)
                    click_log_output.write(line)
                    click_log_output.write('\n')

                for i in range(1, 10):
                    _all_positive_sampling(ip, device_id, query_group_id, ad_id_query, query_camp_ad,
                                           click_log_output)

        # negative
        for x in range(2, 5):
            device_id = level_1_user[ip][x]
            query_group = {1: 0.7, 0: 0.3}
            for query_group_id in query_camp_ad:
                positive = _weighted_sampling(query_group)
                if positive == 1:
                    for i in range(1, 50):
                        fields = _positive_sampling(ip, device_id, query_group_id, ad_id_query, query_camp_ad,
                                                    campaign_weight, ad_weight)
                        if not _valid(fields):
                            continue
                        line = ",".join(fields)
                        click_log_output.write(line)
                        click_log_output.write('\n')
                else:
                    for entry in ad_list:
                        if entry["query_group_id"] == query_group_id:
                            negative_type = _weighted_sampling(negative_types)
                            fields = _negative_sampling(ip, device_id, entry, query_camp_ad, campaign_weight,
                                                        ad_weight,
                                                        campaign_id_category, campaign_id_ad_id,
                                                        negative_type)
                            if not _valid(fields):
                                continue
                            line = ",".join(fields)
                            click_log_output.write(line)
                            click_log_output.write('\n')

    for ip in level_2_user:
        for query_group_id in query_camp_ad:
            millis = int(round(time.time() * 1000))
            random.seed(millis)
            r = random.randint(0, 1)
            if r == 1:
                # positive sample
                for i in range(1, 10):
                    fields = _positive_sampling(ip, level_2_user[ip][0], query_group_id, ad_id_query,
                                                query_camp_ad, campaign_weight, ad_weight)
                    if not _valid(fields):
                        continue
                    line = ",".join(fields)
                    click_log_output.write(line)
                    click_log_output.write('\n')

            else:
                # negative_type sample
                for entry in ad_list:
                    if entry["query_group_id"] == query_group_id:
                        negative_type = _weighted_sampling(negative_types)
                        fields = _negative_sampling(ip, level_2_user[ip][0], entry, query_camp_ad,
                                                    campaign_weight,
                                                    ad_weight, campaign_id_category, campaign_id_ad_id,
                                                    negative_type)
                        if not _valid(fields):
                            continue
                        line = ",".join(fields)
                        click_log_output.write(line)
                        click_log_output.write('\n')

            for i in range(1, 5):
                device_id = level_2_user[ip][i]
                _all_positive_sampling(ip, device_id, query_group_id, ad_id_query, query_camp_ad,
                                       click_log_output)

            # negative
            for i in range(1, 5):
                device_id = level_2_user[ip][i]
                for j in range(1, 4):
                    for entry in ad_list:
                        negative_type = _weighted_sampling(negative_types)
                        fields = _negative_sampling(ip, device_id, entry, query_camp_ad, campaign_weight,
                                                    ad_weight,
                                                    campaign_id_category, campaign_id_ad_id, negative_type)
                        if not _valid(fields):
                            continue
                        line = ",".join(fields)
                        click_log_output.write(line)
                        click_log_output.write('\n')

    for ip in level_3_user:
        for device_id in level_3_user[ip]:
            for entry in ad_list:
                negative_type = _weighted_sampling(negative_types)
                fields = _negative_sampling(ip, device_id, entry, query_camp_ad, campaign_weight, ad_weight,
                                            campaign_id_category, campaign_id_ad_id, negative_type)
                if not _valid(fields):
                    continue
                line = ",".join(fields)
                click_log_output.write(line)
                click_log_output.write('\n')

    click_log_output.close()
    logger = logging.getLogger()
    logger.info("Click log generation finished")


if __name__ == "__main__":
    # Device IP, Device id,Session id,Query,AdId,CampaignId,Ad_category_Query_category(0/1),clicked(0/1)
    # use following feature to generate click Log
    # IP, device_id, AdId,QueryCategry_AdsCategory,Query_CampaignId, Query_AdId

    file_dir = sys.argv[1]

    generate_click_log(file_dir)
