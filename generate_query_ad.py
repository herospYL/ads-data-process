import json
import sys
import logging
from itertools import islice

AD_FILE = "ad_file.txt"
QUERY_CAMP_AD_FILE = "query_camp_ad_file.txt"
CAMPAIGN_WEIGHT_FILE = "campaign_weight_file.txt"
AD_WEIGHT_FILE = "ad_weight_file.txt"
QUERY_GROUP_ID_QUERY_FILE = "query_group_id_query_file.txt"
CAMPAIGN_ID_CATEGORY_FILE = "campaign_id_category_file.txt"
CAMPAIGN_ID_AD_ID_FILE = "campaign_id_ad_id_file.txt"


def window(seq, n=2):
    # Returns a sliding window (of width n) over data from the iterable
    #   s -> (s0,s1,...s[n-1]), (s1,s2,...,sn), ...
    it = iter(seq)
    result = tuple(islice(it, n))
    if len(result) == n:
        yield result
    for elem in it:
        result = result[1:] + (elem,)
        yield result


def ngrams(tokens, size):
    for i in range(size):
        for t in window(tokens, i + 1):
            yield " ".join(t)


def calculate_relevance_score(query, keywords):
    keyword_set = set()
    for keyword in keywords:
        keyword_set.add(keyword)

    count_matched = 0
    token_list = query.split(" ")
    for token in token_list:
        if token in keyword_set:
            count_matched += 1

    score = count_matched * 1.0 / len(keyword_set)
    return score


def generate_query_ad(file_dir):
    ad_input_file = file_dir + AD_FILE
    query_camp_ad_file = file_dir + QUERY_CAMP_AD_FILE
    campaign_weight_file = file_dir + CAMPAIGN_WEIGHT_FILE
    ad_weight_file = file_dir + AD_WEIGHT_FILE
    query_group_id_query_file = file_dir + QUERY_GROUP_ID_QUERY_FILE
    campaign_id_category_file = file_dir + CAMPAIGN_ID_CATEGORY_FILE
    campaign_id_ad_id_file = file_dir + CAMPAIGN_ID_AD_ID_FILE

    query_camp_ad = {}
    campaign_weight = {}
    ad_weight = {}
    query_group_id_query = {}
    campaign_id_category = {}
    campaign_id_ad_id = {}

    logger = logging.getLogger()
    with open(ad_input_file, "r") as lines:
        for line in lines:
            entry = json.loads(line.strip())
            if "category" in entry \
                    and "query" in entry \
                    and "campaignId" in entry \
                    and "query_group_id" in entry \
                    and "keyWords" in entry \
                    and "adId" in entry:
                query = entry["query"].lower()
                campaign_id = entry["campaignId"]
                ad_id = entry["adId"]
                query_group_id = entry["query_group_id"]
                keywords = entry["keyWords"]
                query_ad_category = entry["category"].lower()
                relevance_score = calculate_relevance_score(query, keywords)
                if query_group_id in query_camp_ad:
                    if campaign_id in query_camp_ad[query_group_id]:
                        query_camp_ad[query_group_id][campaign_id].append(ad_id)
                        campaign_id_ad_id[campaign_id].append(ad_id)
                        ad_weight[ad_id] = relevance_score
                    else:
                        query_camp_ad[query_group_id][campaign_id] = []
                        query_camp_ad[query_group_id][campaign_id].append(ad_id)
                        campaign_id_ad_id[campaign_id] = []
                        campaign_id_ad_id[campaign_id].append(ad_id)
                        ad_weight[ad_id] = relevance_score
                else:
                    query_camp_ad[query_group_id] = {}
                    query_camp_ad[query_group_id][campaign_id] = []
                    query_camp_ad[query_group_id][campaign_id].append(ad_id)
                    campaign_id_ad_id[campaign_id] = []
                    campaign_id_ad_id[campaign_id].append(ad_id)
                    ad_weight[ad_id] = relevance_score

                if query_group_id in query_group_id_query:
                    query_group_id_query[query_group_id][query] = 1
                else:
                    query_group_id_query[query_group_id] = {}

                campaign_id_category[campaign_id] = query_ad_category

    for query_group_id in query_camp_ad:
        total_relevance_cross_camp = 0.0
        for camp_id in query_camp_ad[query_group_id]:
            total_ad_relevance_per_camp = 0.0
            for ad_id in query_camp_ad[query_group_id][camp_id]:
                total_ad_relevance_per_camp += ad_weight[ad_id]
                logger.debug("total_ad_relevance_per_camp:{0}".format(total_ad_relevance_per_camp))

            for ad_id in query_camp_ad[query_group_id][camp_id]:
                if total_ad_relevance_per_camp > 0.0:
                    ad_weight[ad_id] = ad_weight[ad_id] / total_ad_relevance_per_camp

            total_relevance_cross_camp += total_ad_relevance_per_camp
            campaign_weight[camp_id] = total_ad_relevance_per_camp
            logger.debug("campaign_weight[camp_id]:{0}".format(campaign_weight[camp_id]))

        for camp_id in query_camp_ad[query_group_id]:
            if total_relevance_cross_camp > 0.0:
                campaign_weight[camp_id] = campaign_weight[camp_id] / total_relevance_cross_camp

    with open(query_camp_ad_file, 'w') as fp:
        json.dump(query_camp_ad, fp)

    with open(campaign_weight_file, 'w') as fp:
        json.dump(campaign_weight, fp)

    with open(ad_weight_file, 'w') as fp:
        json.dump(ad_weight, fp)

    with open(query_group_id_query_file, 'w') as fp:
        json.dump(query_group_id_query, fp)

    with open(campaign_id_category_file, 'w') as fp:
        json.dump(campaign_id_category, fp)

    with open(campaign_id_ad_id_file, 'w') as fp:
        json.dump(campaign_id_ad_id, fp)

    logger.info("Query ad generation finished")


if __name__ == "__main__":
    file_dir = sys.argv[1]

    generate_query_ad(file_dir)
