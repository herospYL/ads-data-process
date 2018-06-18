import json
import sys
import logging


# input： [nike, running, shoes] , Dict
# output: all rewrite query
def _query_rewriter_helper(query_terms, synonyms_dict):
    if len(query_terms) == 0:
        return []

    if len(query_terms) == 1:
        if query_terms[0] not in synonyms_dict:
            return [query_terms[0]]
        else:
            return list(synonyms_dict[query_terms[0]])

    prev = _query_rewriter_helper(query_terms[:-1], synonyms_dict)
    if query_terms[-1] in synonyms_dict:
        post = synonyms_dict[query_terms[-1]]
        return [s + '_' + c for s in prev for c in post]  # Permutation
    else:
        return [s + '_' + query_terms[-1] for s in prev]


def generate_synonym(synonyms_input_file, ads_input_file, synonyms_output_file, logger):
    synonyms_dict = {}
    query_set = set()
    synonyms_output = {}

    with open(synonyms_input_file, "r") as lines:
        for line in lines:
            entry = json.loads(line.strip())
            if "word" in entry and "synonyms" in entry:
                synonyms_dict[entry["word"]] = entry["synonyms"]
                synonyms_dict[entry["word"]].append(entry["word"])

    with open(ads_input_file, "r") as lines:
        for line in lines:
            entry = json.loads(line.strip())
            if "query" in entry:
                if entry["query"] not in query_set:
                    query_set.add(entry["query"])

    for query in query_set:
        query_terms = query.split(" ")
        query_key = "_".join(query_terms)
        synonyms = _query_rewriter_helper(query_terms, synonyms_dict)
        # dedupe synonyms
        unique_synonyms = set()
        final_synonyms = []
        for synonym in synonyms:
            # print synonym
            if synonym not in unique_synonyms:
                unique_synonyms.add(synonym)
                final_synonyms.append(synonym)

        synonyms_output[query_key] = final_synonyms

    with open(synonyms_output_file, 'w') as fp:
        json.dump(synonyms_output, fp)

    logger.info("Synonyms data generation finished")


if __name__ == "__main__":
    synonyms_input_file = sys.argv[1]
    ads_input_file = sys.argv[2]
    synonyms_output_file = sys.argv[3]

    logger = logging.getLogger()
    generate_synonym(synonyms_input_file, ads_input_file, synonyms_output_file, logger)
