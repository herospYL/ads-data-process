import json
import sys
import logging

from pyspark import SparkContext
from pyspark.mllib.feature import Word2Vec


def word2vec(training_file, synonyms_data_file, trace_file, logger):
    sc = SparkContext(appName="word2vec")
    inp = sc.textFile(training_file).map(lambda line: line.encode("utf8", "ignore").split(" "))

    word2vec = Word2Vec()
    model = word2vec.setLearningRate(0.02).setMinCount(5).setVectorSize(10).setSeed(2017).fit(inp)

    vec = model.getVectors()
    synonyms_data = open(synonyms_data_file, "w")

    logger.debug("len of vec", len(vec))
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
    model.save(sc, trace_file)
    sc.stop()

    logger.info("Word2Vec training finished")


if __name__ == "__main__":
    training_file = sys.argv[1]
    synonyms_data_file = sys.argv[2]
    trace_file = sys.argv[3]

    logger = logging.getLogger()
    word2vec(training_file, synonyms_data_file, training_file, logger)
