# ============================================================
# END TO END RAG WITH QUERY EXPANSION (NO USER FUNCTIONS)
# LangChain v1+
# ============================================================

# INPUT
# ------------------------------------------------------------
# User Question:
# What does LangChain support?
#
# OUTPUT
# ------------------------------------------------------------
# LangChain supports RAG pipelines, vector databases,
# prompt engineering, agents, structured output and
# integrations with LLM providers.


# ============================================================
# 1. Install Packages
# ============================================================

# pip install langchain langchain-openai faiss-cpu


# ============================================================
# 2. Imports
# ============================================================

import random
from langchain_openai import AzureChatOpenAI, AzureOpenAIEmbeddings
from langchain.vectorstores import FAISS
from langchain.schema import Document
from langchain.prompts import ChatPromptTemplate


# ============================================================
# 3. Dataset (LangChain + Random Sales Data)
# ============================================================

langchain_docs = [
"LangChain is a framework for building LLM powered applications.",
"LangChain supports retrieval augmented generation pipelines.",
"LangChain integrates with vector databases like FAISS, Chroma and Pinecone.",
"LangChain provides tools for prompt engineering and chaining.",
"LangChain agents allow LLMs to interact with external tools.",
"LangChain supports structured output using Pydantic and TypedDict.",
"LangChain allows integration with OpenAI, Azure OpenAI and other models.",
"LangChain provides memory modules for chat applications.",
"LangChain enables document loaders for ingesting data.",
"LangChain helps build conversational AI systems."
]

sales_docs = [
"Ram sold 200 units in Pune during Jan 2026.",
"Ram sold 180 units in Pune during Feb 2026.",
"Ram sold 220 units in Pune during Mar 2026.",
"Anita sold 300 units in Bangalore during Jan 2026.",
"Anita sold 280 units in Bangalore during Feb 2026.",
"Anita sold 310 units in Bangalore during Mar 2026.",
"Shyam sold 150 units in Mumbai during Jan 2026.",
"Shyam sold 170 units in Mumbai during Feb 2026.",
"Shyam sold 160 units in Mumbai during Mar 2026.",
"Ravi sold 250 units in Hyderabad during Jan 2026.",
"Ravi sold 240 units in Hyderabad during Feb 2026.",
"Ravi sold 260 units in Hyderabad during Mar 2026."
]


# ============================================================
# 4. Combine & Shuffle Dataset
# ============================================================

docs = langchain_docs + sales_docs

random.shuffle(docs)


# ============================================================
# 5. Convert to Documents
# ============================================================

documents = [Document(page_content=d) for d in docs]


# ============================================================
# 6. Embedding Model
# ============================================================

embeddings = AzureOpenAIEmbeddings(
    model="text-embedding-3-small"
)


# ============================================================
# 7. Create Vector Store
# ============================================================

vectorstore = FAISS.from_documents(documents, embeddings)

retriever = vectorstore.as_retriever(search_kwargs={"k":4})


# ============================================================
# 8. LLM
# ============================================================

llm = AzureChatOpenAI(
    deployment_name="gpt-4o-mini",
    temperature=0
)


# ============================================================
# 9. USER QUESTION
# ============================================================

question = "What does LangChain support?"

print("\nUser Question:")
print(question)


# ============================================================
# 10. QUERY EXPANSION
# ============================================================

query_prompt = ChatPromptTemplate.from_template("""
You are an AI assistant.

Generate 3 alternative search queries for the user question.

Question:
{question}

Return queries separated by new lines.
""")

query_prompt_value = query_prompt.invoke({"question": question})

query_response = llm.invoke(query_prompt_value)

expanded_queries = query_response.content.split("\n")

expanded_queries = [q.strip("- ") for q in expanded_queries if q.strip()]

print("\nExpanded Queries:")
for q in expanded_queries:
    print("-", q)


# ============================================================
# 11. RETRIEVE DOCUMENTS
# ============================================================

retrieved_docs = []

for q in expanded_queries:
    results = retriever.invoke(q)
    retrieved_docs.extend(results)


# Remove duplicates
unique_docs = list({doc.page_content: doc for doc in retrieved_docs}.values())


# ============================================================
# 12. FORMAT CONTEXT
# ============================================================

context = "\n".join([doc.page_content for doc in unique_docs])

print("\nRetrieved Context:")
print(context)


# ============================================================
# 13. FINAL RAG PROMPT
# ============================================================

rag_prompt = ChatPromptTemplate.from_template("""
Answer the question using the context.

Context:
{context}

Question:
{question}
""")

rag_prompt_value = rag_prompt.invoke({
    "context": context,
    "question": question
})


# ============================================================
# 14. GENERATE FINAL ANSWER
# ============================================================

final_response = llm.invoke(rag_prompt_value)

print("\nFinal Answer:")
print(final_response.content)
