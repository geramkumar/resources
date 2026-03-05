# ============================================================
# LangChain RAG Learning Script
# Concepts Demonstrated:
# 1. Query Expansion
# 2. Query Decomposition
# 3. HyDE (Hypothetical Document Embedding)
# Embedding Model : Azure OpenAI text-embedding-3-small
# Chat Model      : Azure OpenAI Chat
# ============================================================

# pip install langchain langchain-openai langchain-community faiss-cpu

import random
from langchain.schema import Document
from langchain_openai import AzureOpenAIEmbeddings, AzureChatOpenAI
from langchain_community.vectorstores import FAISS


# ============================================================
# 1. Azure OpenAI Configuration
# ============================================================

AZURE_ENDPOINT = "YOUR_ENDPOINT"
AZURE_API_KEY = "YOUR_KEY"
API_VERSION = "2024-02-15-preview"

EMBEDDING_MODEL = "text-embedding-3-small"
CHAT_DEPLOYMENT = "gpt-4o-mini"


# ============================================================
# 2. Sample Dataset (LangChain + Unrelated)
# ============================================================

related_content = [
"LangChain is a framework used to build applications powered by large language models.",
"LangChain enables retrieval augmented generation pipelines.",
"Embeddings convert text into numerical vectors used in semantic search.",
"Vector databases like FAISS store embeddings for similarity search.",
"LangChain retrievers fetch relevant documents for user queries.",
"Hybrid search combines keyword search and semantic similarity.",
"Cosine similarity measures closeness between embedding vectors.",
"MMR retrieval improves diversity of retrieved documents.",
"RAG systems reduce hallucination by grounding responses in documents.",
"LangChain integrates with OpenAI and Azure OpenAI."
]

unrelated_content = [
"Cricket is the most popular sport in India.",
"The Taj Mahal is located in Agra.",
"Yoga improves flexibility and mental health.",
"Cooking pasta requires boiling water.",
"The Himalayas are the highest mountains.",
"Gardening is a relaxing hobby.",
"Music improves mood and concentration.",
"Reading books improves knowledge.",
"Walking improves cardiovascular health.",
"Swimming is a good exercise."
]

dataset = related_content + unrelated_content
random.shuffle(dataset)

documents = [Document(page_content=text) for text in dataset]

print("\n==============================")
print("Dataset Sample")
print("==============================")

for d in documents[:5]:
    print(d.page_content)


# ============================================================
# 3. Create Embeddings
# ============================================================

embeddings = AzureOpenAIEmbeddings(
    azure_endpoint=AZURE_ENDPOINT,
    api_key=AZURE_API_KEY,
    api_version=API_VERSION,
    model=EMBEDDING_MODEL
)

print("\nEmbedding model initialized")


# ============================================================
# 4. Create Vector Store
# ============================================================

vectorstore = FAISS.from_documents(documents, embeddings)

print("\nVector store created with FAISS")


# ============================================================
# 5. Initialize Azure Chat Model
# ============================================================

llm = AzureChatOpenAI(
    azure_endpoint=AZURE_ENDPOINT,
    api_key=AZURE_API_KEY,
    api_version=API_VERSION,
    deployment_name=CHAT_DEPLOYMENT,
    temperature=0
)

print("\nChat model initialized")


# ============================================================
# 6. Query Expansion
# ============================================================

query = "How does LangChain help build RAG systems?"

print("\n==============================")
print("Original Query")
print("==============================")
print(query)


expansion_prompt = f"""
Generate 3 alternative search queries similar to the question below.

Question: {query}
"""

expanded_queries = llm.invoke(expansion_prompt).content.split("\n")

print("\n==============================")
print("Expanded Queries")
print("==============================")

for q in expanded_queries:
    print(q)


# ============================================================
# 7. Query Decomposition
# ============================================================

decomposition_prompt = f"""
Break the question into smaller sub-questions.

Question: {query}
"""

sub_questions = llm.invoke(decomposition_prompt).content.split("\n")

print("\n==============================")
print("Query Decomposition (Sub Questions)")
print("==============================")

for s in sub_questions:
    print(s)


# ============================================================
# 8. HyDE (Hypothetical Document Embeddings)
# ============================================================

hyde_prompt = f"""
Write a hypothetical answer paragraph for the question below.

Question: {query}
"""

hypothetical_doc = llm.invoke(hyde_prompt).content

print("\n==============================")
print("HyDE Generated Hypothetical Document")
print("==============================")
print(hypothetical_doc)


# ============================================================
# 9. Retrieve using HyDE embedding
# ============================================================

hyde_results = vectorstore.similarity_search(hypothetical_doc, k=4)

print("\n==============================")
print("Retrieved Documents using HyDE")
print("==============================")

for i, doc in enumerate(hyde_results):
    print(f"\nDoc {i+1}")
    print(doc.page_content)


# ============================================================
# 10. Retrieve using Query Expansion
# ============================================================

print("\n==============================")
print("Retrieval using Expanded Queries")
print("==============================")

for q in expanded_queries[:3]:

    docs = vectorstore.similarity_search(q, k=2)

    print("\nQuery:", q)

    for d in docs:
        print("-", d.page_content)


# ============================================================
# 11. Retrieve using Decomposed Queries
# ============================================================

print("\n==============================")
print("Retrieval using Sub Questions")
print("==============================")

for q in sub_questions[:3]:

    docs = vectorstore.similarity_search(q, k=2)

    print("\nSub Question:", q)

    for d in docs:
        print("-", d.page_content)
