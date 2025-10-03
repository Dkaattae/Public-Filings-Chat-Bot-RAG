import pandas as pd
import re
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
    if x.startswith("{") and x.endswith("}"):
        # quote keys that aren't already quoted
        fixed = re.sub(r'(\w+)\s*:', r'"\1":', x)
        try:
            return ast.literal_eval(fixed)
        except (ValueError, SyntaxError):
            return x
    try:
        return ast.literal_eval(x)
    except (ValueError, SyntaxError):
        return x


relevance_total = []
for i, gt in ground_truth.iterrows():
    
    prompt = build_number_search_prompt.number_search_prompt(gt['Question'], \
        gt['Ticker'], build_number_search_prompt.get_schema())
    actual_results = build_number_search_prompt.get_duckdb_results(prompt)
    actual_number_list = list(actual_results[0].values())
    gt_number = parse_value(gt['Answer_number'])
    if type(gt_number) == dict:
        compared_gt_number = list(gt_number.values())[-1]
    else:
        compared_gt_number = gt_number
    # print('gt number: ', compared_gt_number)
    # print('actual: ', actual_results)
    relevance = 0
    if type(compared_gt_number) == int:
        for actual_number in actual_number_list:
            if type(actual_number) in [int, float]:
                try:
                    if abs((actual_number - compared_gt_number) / compared_gt_number) < 0.01:
                        relevance = 1
                except TypeError:
                    continue
            else: 
                continue
    relevance_total.append(relevance)
    
    time.sleep(5)

number_accuracy = accuracy(relevance_total)
print('number search accuracy is: ', number_accuracy)

con.close()