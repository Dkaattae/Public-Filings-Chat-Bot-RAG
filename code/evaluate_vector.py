import json
from tqdm.auto import tqdm
import vector_search

def hit_rate(relevance_total):
    cnt = 0

    for line in relevance_total:
        if True in line:
            cnt = cnt + 1

    return cnt / len(relevance_total)

def mrr(relevance_total):
    total_score = 0.0

    for line in relevance_total:
        for rank in range(len(line)):
            if line[rank] == True:
                total_score = total_score + 1 / (rank + 1)

    return total_score / len(relevance_total)

def evaluate(ground_truth, search_function):
    relevance_total = []

    for q in tqdm(ground_truth):
        doc_id = q['_dlt_id']
        results = search_function(q)
        relevance = [d.payload['_dlt_id'] == doc_id for d in results]
        relevance_total.append(relevance)

    return {
        'hit_rate': hit_rate(relevance_total),
        'mrr': mrr(relevance_total),
    }

ground_truth_path = '../files/vector_ground_truth.json'
with open(ground_truth_path, "r", encoding="utf-8") as f:
    ground_truth = json.load(f)

# business_rows = [row for row in ground_truth if row.get("section") == "Business"]
qdrant_results = evaluate(ground_truth, \
    lambda q: vector_search.vector_search(q['question'], q['ticker'], [q['year']], limit=3))

print('hit rate: ', qdrant_results['hit_rate'])
print('mrr: ', qdrant_results['mrr'])