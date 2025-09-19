import os
import json
from qdrant_client import QdrantClient
from qdrant_client.http.models import models

collection_name = "public_filing_data_public_filings_docs"
output_file = "../files/vector_ground_truth.json"
top_k = 5  # only relevant if you later want to filter top K docs
qd_client = QdrantClient(host="localhost", port=6333)

# Fetch all points from the collection
# Assuming you have a small to medium collection (otherwise use paging)
all_points = []
offset = None

while True:
    points, offset = qd_client.scroll(
        collection_name=collection_name,
        with_payload=True,
        with_vectors=False,   # skip vectors for speed unless needed
        offset=offset
    )
    all_points.extend(points)

    if offset is None:  # no more pages
        break

print(len(all_points))

ground_truth = []
with open('../files/ground_truth_text_questions.json', "r") as f:
    section_questions = json.load(f)

for doc in all_points:
    payload = doc.payload
    text = payload.get("text")
    ticker = payload.get("ticker")
    year = payload.get("year")
    section = payload.get("section")
    _dlt_id = payload.get("_dlt_id")  # DLT internal ID
    questions = []
    for item in section_questions: 
        if item["section"].lower() == section.lower():
            questions = item['questions']
    if len(questions) == 0:
        questions.append(f"please summarize {section} section".format(section=section))
    for q in questions:
        gt_entry = {
            "question": q,
            "ticker": [ticker],
            "year": year,
            "_dlt_id": _dlt_id,
            "retriever": "qdrant",
            "filing_type": payload.get("filing_type"),
            "section": payload.get("section")
        }
        ground_truth.append(gt_entry)

# Save to JSON
with open(output_file, "w") as f:
    json.dump(ground_truth, f, indent=2)

print(f"Ground truth saved to {output_file}, total questions: {len(ground_truth)}")