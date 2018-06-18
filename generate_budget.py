import json
import random
import sys
import logging


def generate_budget(output_file, logger):
    output = open(output_file, "w")
    template = '{"campaignId":8001,"budget":1500}'

    for i in range(8001, 8900):
        entry = json.loads(template.strip())
        entry["campaignId"] = i
        entry["budget"] = random.randint(100, 2000)  # Generate random budget
        output.write(json.dumps(entry))
        output.write('\n')

    output.close()
    logger.info("Budget generation finished")


if __name__ == "__main__":
    output_file = sys.argv[1]

    logger = logging.getLogger()
    generate_budget(output_file, logger)
