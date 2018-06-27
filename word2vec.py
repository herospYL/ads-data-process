import json
import sys
import logging

from pyspark import SparkContext
from pyspark.mllib.feature import Word2Vec

from generate_word2vec_training_data import WORD2VEC_TRAINING_FILE

WORD2VEC_TRACE = "word2vec_trace"
SYNONYM_DATA_FILE = "synonym_data_file.txt"


def word2vec(file_dir, logger):
    word2vec_training_file = file_dir + WORD2VEC_TRAINING_FILE
    synonym_data_file = file_dir + SYNONYM_DATA_FILE
    word2vec_trace_data = file_dir + WORD2VEC_TRACE

    sc = SparkContext(appName="word2vec")
    inp = sc.textFile(word2vec_training_file).map(lambda line: line.split(" "))

    word2vec = Word2Vec()
    model = word2vec.setLearningRate(0.02).setMinCount(5).setVectorSize(10).setSeed(2017).fit(inp)

    vec = model.getVectors()
    synonyms_data = open(synonym_data_file, "w")

    logger.debug("len of vec:{0}".format(len(vec)))
    for word in vec.keys():
        synonyms = model.findSynonyms(word, 5)
        entry = {"word": word}
        synon_list = []
        for synonym, cosine_distance in synonyms:
            synon_list.append(synonym)
        entry["synonyms"] = synon_list
        synonyms_data.write(json.dumps(entry))
        synonyms_data.write('\n')

    synonyms_data.close()
    model.save(sc, word2vec_trace_data)
    sc.stop()

    logger.info("Word2Vec training finished")


if __name__ == "__main__":
    file_dir = sys.argv[1]

    logger = logging.getLogger()
    word2vec(file_dir, logger)
