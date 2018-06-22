import shutil
import sys
import logging
import boto3

from generate_budget import generate_budget
from generate_query_ad import (generate_query_ad, AD_FILE)
from generate_word2vec_training_data import generate_word2vec_training_data
from word2vec import word2vec
from generate_synonym import (generate_synonym, SYNONYM_STORE_FILE)
from generate_click_log import (generate_click_log, USER_FILE)
from select_feature import select_feature
from store_feature import (store_feature, FEATURE_STORE_FILE)
from prepare_ctr_training_data import prepare_ctr_training_data
from ctr_train import (ctr_logistic, ctr_gbdt, CTR_LOGISTIC_DATA, CTR_GBDT_DATA)


def set_logger(file_dir, debug_mode):
    log_format = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
    logger = logging.getLogger()
    if debug_mode is not None and debug_mode.lower() == "-debug":
        logger.setLevel(logging.DEBUG)

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(log_format)
    logger.addHandler(ch)

    fh = logging.FileHandler("{0}/{1}.log".format(file_dir, "ads_data_process"))
    fh.setFormatter(log_format)
    logger.addHandler(fh)

    return logger


if __name__ == "__main__":
    file_dir = sys.argv[1]
    raw_ads_path = sys.argv[2]
    users_path = sys.argv[3]
    bucket = sys.argv[4]
    debug_mode = sys.argv[5]

    logger = None
    try:
        logger = set_logger(file_dir, debug_mode)
        s3 = boto3.client('s3')

        if not file_dir.endswith('/'):
            file_dir = file_dir + '/'

        raw_ads_dest = file_dir + AD_FILE
        users_dest = file_dir + USER_FILE

        shutil.copy(raw_ads_path, raw_ads_dest)
        shutil.copy(users_path, users_dest)

        generate_budget(file_dir, logger)
        generate_query_ad(file_dir, logger)
        generate_word2vec_training_data(file_dir, logger)
        word2vec(file_dir, logger)
        generate_synonym(file_dir, logger)
        generate_click_log(file_dir, logger)
        select_feature(file_dir, logger)
        store_feature(file_dir, logger)
        prepare_ctr_training_data(file_dir, logger)
        ctr_logistic(file_dir, logger)
        ctr_gbdt(file_dir, logger)

        s3.upload_file(file_dir + SYNONYM_STORE_FILE, bucket, SYNONYM_STORE_FILE)
        s3.upload_file(file_dir + FEATURE_STORE_FILE, bucket, FEATURE_STORE_FILE)
        # Need to know CTR training data file path
        # s3.upload_file(file_dir + CTR_LOGISTIC_DATA + "?", bucket, CTR_LOGISTIC_DATA + "?")
        # s3.upload_file(file_dir + CTR_GBDT_DATA + "?", bucket, CTR_GBDT_DATA + "?")

    except Exception as e:
        logger.error(e)
