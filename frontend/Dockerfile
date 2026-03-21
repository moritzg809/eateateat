FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-download the Jina embedding model so the first search request is instant.
# This bakes the model weights (~300 MB) into the image layer.
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('jinaai/jina-embeddings-v5-text-small-retrieval', trust_remote_code=True); print('Jina model cached.')"

COPY . .

CMD ["python", "app.py"]
