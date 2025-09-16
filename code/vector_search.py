import os
from fastembed import TextEmbedding
from qdrant_client import QdrantClient, models
    
qd_client = QdrantClient(host="localhost", port=6333)
collection_name = "public_filing_data_public_filings_docs"
embed_model = TextEmbedding(model_name="BAAI/bge-small-en-v1.5")

def vector_search(sentence, ticker, limit=1):
    qdrant_filter = None
    if ticker:
        qdrant_filter = models.Filter(
            must=[
                models.FieldCondition(
                    key="ticker",
                    match=models.MatchValue(value=ticker)
                )
            ]
        )
    query_embedding = embed_model.embed([sentence])
    query_vector = list(query_embedding)[0]

    query_points = qd_client.search(
        collection_name=collection_name,
        query_vector=models.NamedVector(
            name="fast-bge-small-en",  
            vector=query_vector       
        ),
        query_filter=qdrant_filter,
        limit=limit,
        with_payload=True
        )
    return query_points

if __name__ == "__main__":
    question = 'what did tesla report in 2024?'
    ticker = 'TSLA'
    related_docs = vector_search(question, ticker)
    print(related_docs)