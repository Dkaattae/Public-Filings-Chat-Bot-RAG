# Edgar RAG
## TODO
placeholders for existing code base   
data pipeline   
qdrant   
rag pipeline   
evaluation/MRR and hit rate    
   
need some research   
evaluation/ground truth   
monitoring   
UI   
maybe MCP   

## data source
100 nasdaq companies, dated back to 2024 to make data shorter.   
text files from edgar 10k, chunk into sections   
text files from edgar 8k, whole text    
text files from edgar 10q, ???   
xbrl data into postgres database   

## data pipeline
kestra to download files and put into S3   
dlt download xbrl data from yahoo and load into postgres   

## vector database
qdrant vector database    

## evaluataion
ground truth file   


## monoriting
???   