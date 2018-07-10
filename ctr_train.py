import json
import logging
import sys

from pyspark import SparkContext
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import GradientBoostedTrees

from prepare_ctr_training_data import CTR_TRAINING_DATA

CTR_GBDT_DATA = "ctr_gbdt_data"
CTR_LOGISTIC_DATA = "ctr_logistic_data"
CTR_GBDT_STORE_FILE = "ctr_gbdt_store_file.txt"
CTR_LOGISTIC_STORE_FILE = "ctr_logistic_store_file.txt"


def _parse_point(line):
    line = line.strip("()")
    fields = line.split(',')
    features_raw = fields[0:11]  # 0 to 10
    features = []
    for x in features_raw:
        feature = float(x.strip().strip("'").strip())
        features.append(feature)

    label = float(fields[11])
    return LabeledPoint(label, features)


def ctr_gbdt(file_dir):
    sc = SparkContext(appName="CTRGBDTRegression")

    path = file_dir + CTR_TRAINING_DATA + "/part*"
    data = sc.textFile(path)
    (training_data, testData) = data.randomSplit([0.7, 0.3])
    parsed_train_data = training_data.map(_parse_point)
    parsed_test_data = testData.map(_parse_point)

    # Train a GradientBoostedTrees model.
    #  Notes: (a) Empty categoricalFeaturesInfo indicates all features are continuous.
    #         (b) Use more iterations in practice.
    model = GradientBoostedTrees.trainClassifier(parsed_train_data,
                                                 categoricalFeaturesInfo={}, numIterations=100)

    # Evaluate model on test instances and compute test error
    predictions = model.predict(parsed_test_data.map(lambda x: x.features))
    labels_and_predictions = parsed_test_data.map(lambda lp: lp.label).zip(predictions)
    test_err = labels_and_predictions.filter(lambda vp: vp[0] != vp[1]).count() / float(parsed_test_data.count())

    logger = logging.getLogger()
    logger.debug('GBDT Training Error = ' + str(test_err))
    logger.debug('Learned classification GBT model:')
    logger.debug(model.toDebugString())
    logger.debug("Tree totalNumNodes" + str(model.totalNumNodes()))

    # Save and load model
    ctr_gbdt_data = file_dir + CTR_GBDT_DATA
    model.save(sc, ctr_gbdt_data)

    logger.info("GBDT training finished")


def ctr_logistic(file_dir):
    sc = SparkContext(appName="CTRLogisticRegression")

    path = file_dir + CTR_TRAINING_DATA + "/part*"
    data = sc.textFile(path)
    (trainingData, testData) = data.randomSplit([0.7, 0.3])

    parsed_train_data = trainingData.map(_parse_point)
    parsed_test_data = testData.map(_parse_point)

    # Build the model
    model = LogisticRegressionWithLBFGS.train(parsed_train_data, intercept=False)

    # Evaluating the model on training data
    labels_and_preds = parsed_test_data.map(lambda p: (p.label, model.predict(p.features)))
    train_err = labels_and_preds.filter(lambda vp: vp[0] != vp[1]).count() / float(parsed_test_data.count())

    logger = logging.getLogger()
    logger.debug("Logistic Training Error = " + str(train_err))
    weights = model.weights
    bias = model.intercept
    logger.debug("weight = ", weights)
    logger.debug("bias =", bias)

    # Save and load model
    ctr_logistic_data = file_dir + CTR_LOGISTIC_DATA
    model.save(sc, ctr_logistic_data)

    # Save as JSON file
    entry = {}
    entry['weights'] = weights.toArray().tolist()
    entry['bias'] = bias

    ctr_logistic_store = file_dir + CTR_LOGISTIC_STORE_FILE
    output = open(ctr_logistic_store, "w")
    output.write(json.dumps(entry))

    logger.info("Logistic Regression training finished")


if __name__ == "__main__":
    file_dir = sys.argv[1]

    # ctr_gbdt(file_dir)
    ctr_logistic(file_dir)
