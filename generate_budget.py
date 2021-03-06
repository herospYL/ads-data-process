import json
import random
import sys
import logging

BUDGET_FILE = "budget_file.txt"


def generate_budget(file_dir):
    output_file = file_dir + BUDGET_FILE
    output = open(output_file, "w")
    template = '{"campaignId":8001,"budget":1500}'

    for i in range(8001, 8900):
        entry = json.loads(template.strip())
        entry["campaignId"] = i
        entry["budget"] = random.randint(100, 2000)  # Generate random budget
        output.write(json.dumps(entry))
        output.write('\n')

    output.close()
    logger = logging.getLogger()
    logger.info("Budget generation finished")


if __name__ == "__main__":
    file_dir = sys.argv[1]

    generate_budget(file_dir)
