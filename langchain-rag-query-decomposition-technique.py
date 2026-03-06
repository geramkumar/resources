#Input:
#What sales did Ram report in Pune in Jan and Feb?

#Decomposed Queries
#1 What sales did Ram report in Pune in Jan 2026?
#2 What sales did Ram report in Pune in Feb 2026?

#Retrieved Context
#Ram sold 200 units in Pune during Jan 2026.
#Ram sold 180 units in Pune during Feb 2026.

# ============================================================
# RAG WITH QUERY DECOMPOSITION
# (LCEL used in Final Answer Stage)
# ============================================================

# pip install langchain langchain-openai faiss-cpu


import random
from langchain_openai import AzureChatOpenAI, AzureOpenAIEmbeddings
from langchain.vectorstores import FAISS
from langchain.schema import Document
from langchain.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser


# ============================================================
# DATASET
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
"Shyam sold 150 units in Mumbai during Jan 2026.",
"Shyam sold 170 units in Mumbai during Feb 2026.",
"Ravi sold 250 units in Hyderabad during Jan 2026."
]


docs = langchain_docs + sales_docs
random.shuffle(docs)

documents = [Document(page_content=d) for d in docs]


# ============================================================
# VECTOR STORE
# ============================================================

embeddings = AzureOpenAIEmbeddings(
    model="text-embedding-3-small"
)

vectorstore = FAISS.from_documents(documents, embeddings)

retriever = vectorstore.as_retriever(search_kwargs={"k":3})


# ============================================================
# LLM
# ============================================================

llm = AzureChatOpenAI(
    deployment_name="gpt-4o-mini",
    temperature=0
)


# ============================================================
# USER QUESTION
# ============================================================

question = "What sales did Ram report in Pune in Jan and Feb?"

print("\nUser Question:")
print(question)


# ============================================================
# QUERY DECOMPOSITION PROMPT
# ============================================================

decomposition_prompt = ChatPromptTemplate.from_template("""
Break the question into smaller independent sub-questions
that can be answered separately.

Question:
{question}

Return each sub-question in a new line.
""")


prompt_value = decomposition_prompt.invoke({"question": question})

decomposition_response = llm.invoke(prompt_value)

sub_queries = decomposition_response.content.split("\n")

sub_queries = [q.strip("- ") for q in sub_queries if q.strip()]


print("\nSub Queries:")
for q in sub_queries:
    print("-", q)


# ============================================================
# RETRIEVE CONTEXT FOR EACH SUBQUERY
# ============================================================

retrieved_docs = []

for q in sub_queries:
    results = retriever.invoke(q)
    retrieved_docs.extend(results)


# Remove duplicates
unique_docs = list({doc.page_content: doc for doc in retrieved_docs}.values())


# ============================================================
# FORMAT CONTEXT
# ============================================================

context = "\n".join([doc.page_content for doc in unique_docs])

print("\nRetrieved Context:")
print(context)


# ============================================================
# FINAL RAG PROMPT
# ============================================================

rag_prompt = ChatPromptTemplate.from_template("""
Answer the question using the context.

Context:
{context}

Question:
{question}
""")


# ============================================================
# LCEL CHAIN (FINAL STAGE)
# ============================================================

rag_chain = (
    rag_prompt
    | llm
    | StrOutputParser()
)


# ============================================================
# RUN FINAL CHAIN
# ============================================================

result = rag_chain.invoke({
    "context": context,
    "question": question
})


print("\nFinal Answer:")
print(result)
