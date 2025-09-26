import pandas as pd
import ast
import time
import random
import duckdb
import build_number_search_prompt

con = duckdb.connect("company_public_info.duckdb")

number_file_path = "../files/ground_truth_number.csv"
ground_truth = pd.read_csv(number_file_path)


def accuracy(relevance_total):
    return sum(relevance_total) / len(relevance_total)


def parse_value(answer):
    x = str(answer).strip()
    if x.endswith("%"):
        try:
            return float(x.strip("%")) / 100
        except ValueError:
            return x
    if x.replace(".", "", 1).isdigit():
        return float(x) if "." in x else int(x)
    try:
        return ast.literal_eval(x)
    except (ValueError, SyntaxError):
        return x


relevance_total = []
for i, gt in ground_truth.iterrows():
    if i <= 10:
        continue
    prompt = build_number_search_prompt.number_search_prompt(gt['Question'], \
        gt['Ticker'], build_number_search_prompt.get_schema())
    actual_results = build_number_search_prompt.get_duckdb_results(prompt)
    actual_number = list(actual_results[0].values())[0]
    gt_number = parse_value(gt['Answer_number'])

    if type(gt_number) in [int, float]:
        if abs((actual_number - gt_number) / gt_number) < 0.01:
            relevance = 1
        else:
            relevance = 0
    else: 
        continue
    relevance_total.append(relevance)
    if i > 20:
        break
    time.sleep(5)

number_accuracy = accuracy(relevance_total)
print('number search accuracy is: ', number_accuracy)

con.close()