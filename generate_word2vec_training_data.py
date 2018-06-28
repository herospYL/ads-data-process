import sys
import json
import logging

from generate_query_ad import AD_FILE

WORD2VEC_TRAINING_FILE = "word2vec_training_file.txt"


def generate_word2vec_training_data(file_dir):
    ad_input_file = file_dir + AD_FILE
    word2vec_training_file = file_dir + WORD2VEC_TRAINING_FILE

    word2vec_training = open(word2vec_training_file, "w")

    with open(ad_input_file, "r") as lines:
        for line in lines:
            entry = json.loads(line.strip())
            if "title" in entry and "adId" in entry and "query" in entry:
                title = entry["title"].lower()
                query = entry["query"].lower()

                # remove number from text
                new_query_tokens = []
                query_tokens = query.split(" ")
                for q in query_tokens:
                    if q.isdigit() is False and len(q) > 1:
                        new_query_tokens.append(q)

                new_title_tokens = []
                title_tokens = title.split(" ")
                for t in title_tokens:
                    if t.isdigit() is False and len(t) > 1:
                        new_title_tokens.append(t)
                query = " ".join(new_query_tokens)
                title = " ".join(new_title_tokens)
                word2vec_training.write(query)
                word2vec_training.write(" ")
                word2vec_training.write(title)
                word2vec_training.write('\n')

    word2vec_training.close()
    logger = logging.getLogger()
    logger.info("Word2Vec training data preparation finished")


if __name__ == "__main__":
    file_dir = sys.argv[1]

    generate_word2vec_training_data(file_dir)
