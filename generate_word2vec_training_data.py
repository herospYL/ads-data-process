import sys
import json
import logging


def generate_word2vec_training_data(input_file, word2vec_training_file, logger):
    word2vec_training = open(word2vec_training_file, "w")

    with open(input_file, "r") as lines:
        for line in lines:
            entry = json.loads(line.strip())
            if "title" in entry and "adId" in entry and "query" in entry:
                title = entry["title"].lower().encode('utf-8')
                query = entry["query"].lower().encode('utf-8') # TODO: Encoding necessary?

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
    logger.info("Word2Vec training data preparation finished")


if __name__ == "__main__":
    input_file = sys.argv[1]  # ads data
    word2vec_training_file = sys.argv[2]

    logger = logging.getLogger()
    generate_word2vec_training_data(input_file, word2vec_training_file, logger)
