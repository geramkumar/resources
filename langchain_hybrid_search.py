# ============================================================
# Hybrid Search RAG Example
# Dense Search  : FAISS (Cosine Similarity)
# Sparse Search : BM25 Retriever
# Hybrid Ranker : EnsembleRetriever
# Dataset       : Mixed related + unrelated content
# ============================================================

# pip install langchain langchain-community faiss-cpu openai tiktoken

import random
from langchain.schema import Document
from langchain_community.vectorstores import FAISS
from langchain_community.retrievers import BM25Retriever
from langchain.retrievers import EnsembleRetriever
from langchain_openai import AzureOpenAIEmbeddings
from langchain_openai import AzureChatOpenAI
from langchain.chains import RetrievalQA


# ============================================================
# 1. Create Sample Dataset (LangChain related + unrelated)
# ============================================================

related_content = [
"LangChain is a framework used to build applications powered by large language models.",
"LangChain supports building RAG pipelines using retrievers and vector databases.",
"Document loaders in LangChain help ingest data from PDFs, websites, and text files.",
"LangChain text splitters divide large documents into smaller chunks for embedding.",
"Vector databases like FAISS and Chroma are commonly used in LangChain for similarity search.",
"Embeddings convert text into numerical vectors for semantic search.",
"Retrievers in LangChain fetch relevant documents based on a query.",
"LangChain chains allow developers to combine LLM calls and tools together.",
"Hybrid search combines semantic similarity and keyword matching.",
"LangChain supports integrations with OpenAI, Azure OpenAI, and HuggingFace models.",
"BM25Retriever enables keyword based search in LangChain.",
"EnsembleRetriever allows combining multiple retrieval strategies.",
"RAG stands for Retrieval Augmented Generation.",
"Cosine similarity measures semantic closeness between vectors.",
"Vector stores allow efficient nearest neighbor search for embeddings."
]

unrelated_content = [
"Cricket is one of the most popular sports played in India.",
"Cooking pasta requires boiling water and adding salt.",
"The Taj Mahal is located in Agra and attracts millions of tourists.",
"Yoga improves flexibility and reduces stress.",
"Many people enjoy traveling to hill stations during summer.",
"Walking daily improves cardiovascular health.",
"Gardening is a relaxing hobby for many people.",
"Reading books regularly improves vocabulary and knowledge.",
"Music helps people relax and improve their mood.",
"Baking bread requires flour, yeast, and patience.",
"Football is widely played in Europe and South America.",
"Green tea is believed to improve metabolism.",
"Watching movies is a popular weekend activity.",
"The Himalayas are the highest mountain range in the world.",
"Swimming is a good full body exercise."
]

dataset = related_content + unrelated_content
random.shuffle(dataset)

documents = [Document(page_content=text) for text in dataset]


# ============================================================
# 2. Create Dense Retriever (FAISS + Embeddings)
# ============================================================

embeddings = AzureOpenAIEmbeddings(
    azure_endpoint="YOUR_AZURE_ENDPOINT",
    api_key="YOUR_API_KEY",
    api_version="2024-02-15-preview",
    model="text-embedding-3-small"
)

vectorstore = FAISS.from_documents(documents, embeddings)

dense_retriever = vectorstore.as_retriever(
    search_type="similarity",
    search_kwargs={"k":4}
)


# ============================================================
# 3. Create Sparse Retriever (BM25)
# ============================================================

bm25_retriever = BM25Retriever.from_documents(documents)
bm25_retriever.k = 4


# ============================================================
# 4. Create Hybrid Retriever (Ensemble)
# ============================================================

ensemble_retriever = EnsembleRetriever(
    retrievers=[bm25_retriever, dense_retriever],
    weights=[0.5, 0.5]
)


# ============================================================
# 5. Create LLM
# ============================================================

llm = AzureChatOpenAI(
    azure_endpoint="YOUR_AZURE_ENDPOINT",
    api_key="YOUR_API_KEY",
    api_version="2024-02-15-preview",
    deployment_name="gpt-4o-mini",
    temperature=0
)


# ============================================================
# 6. Create RAG Chain
# ============================================================

qa_chain = RetrievalQA.from_chain_type(
    llm=llm,
    retriever=ensemble_retriever,
    chain_type="stuff"
)


# ============================================================
# 7. Query
# ============================================================

query = "What is LangChain and how is it used in RAG systems?"

response = qa_chain.invoke({"query": query})

print("\nQuestion:\n", query)
print("\nAnswer:\n", response["result"])


# ============================================================
# 8. Inspect Retrieved Documents
# ============================================================

docs = ensemble_retriever.get_relevant_documents(query)

print("\nRetrieved Documents\n")

for i, doc in enumerate(docs):
    print(f"Doc {i+1}: {doc.page_content}")
