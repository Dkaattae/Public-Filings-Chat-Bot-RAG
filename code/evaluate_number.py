import pandas as pd
import random
import duckdb
import build_number_search_prompt

con = duckdb.connect("company_public_info.duckdb")

number_file_path = "../files/ground_truth_number.csv"
ground_truth = pd.read_csv(number_file_path)


def accuracy(relevance_total):
    return sum(relevance_total) / len(relevance_total)

relevance_total = []
for i, gt in ground_truth.iterrows():
    if i > 10:
        break
    prompt = build_number_search_prompt.number_search_prompt(gt['Question'], \
        gt['Ticker'], build_number_search_prompt.get_schema())
    actual_results = build_number_search_prompt.get_duckdb_results(prompt)
    print('question: ', gt['Question'])
    print('actual answer: ', actual_results)
    print('gt answer: ', gt['Answer_number'])
    actual_number = list(actual_results[0].values())[0]
    gt_number = gt['Answer_number']
    if (actual_number - gt_number) / gt_number < 0.01 \
        or (actual_number - gt_number) / gt_number > -0.01:
        relevance = 1
    else: relevance = 0
    relevance_total.append(relevance)
    sleep(5)

number_accuracy = accuracy(relevance_total)
print('number search accuracy is: ', number_accuracy)