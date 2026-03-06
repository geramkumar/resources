https://livingfordatabricks.wordpress.com/2026/03/06/multi-model-rag-overview/

Input: What sales did Ram report in Pune?

Query Rewritten
What sales did Ram report in Pune in 2026?

Retrieved Context
Ram sold 200 units in Pune during Jan 2026.
Ram sold 180 units in Pune during Feb 2026.
Ram sold 220 units in Pune during Mar 2026.

Final Answer
Ram sold 200 units in Pune in Jan 2026,
180 units in Feb 2026,
and 220 units in Mar 2026.


# ============================================================
# MULTI-MODEL RAG PIPELINE
# LangChain v1+
# Azure OpenAI Models
# ============================================================

# pip install langchain langchain-openai faiss-cpu


import random
from langchain_openai import AzureChatOpenAI, AzureOpenAIEmbeddings
from langchain_community.vectorstores import FAISS
from langchain.schema import Document
from langchain.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnableLambda, RunnableParallel


# ============================================================
# 1 SAMPLE DATASET
# ============================================================

docs = [
"LangChain is a framework for building LLM powered applications.",
"LangChain supports retrieval augmented generation pipelines.",
"LangChain integrates with vector databases like FAISS and Pinecone.",
"LangChain provides tools for prompt engineering.",
"Ram sold 200 units in Pune during Jan 2026.",
"Ram sold 180 units in Pune during Feb 2026.",
"Ram sold 220 units in Pune during Mar 2026.",
"Anita sold 300 units in Bangalore during Jan 2026."
]

random.shuffle(docs)

documents = [Document(page_content=d) for d in docs]


# ============================================================
# 2 EMBEDDING MODEL
# ============================================================

embedding_model = AzureOpenAIEmbeddings(
    model="text-embedding-3-small"
)


# ============================================================
# 3 VECTOR DATABASE
# ============================================================

vectorstore = FAISS.from_documents(documents, embedding_model)

retriever = vectorstore.as_retriever(search_kwargs={"k":5})


# ============================================================
# 4 LLM MODELS
# ============================================================

# Query rewriting (cheap model)
query_model = AzureChatOpenAI(
    deployment_name="gpt-4o-mini",
    temperature=0
)

# Strong model for final answer
answer_model = AzureChatOpenAI(
    deployment_name="gpt-4o",
    temperature=0
)

# Safety moderation model
moderation_model = AzureChatOpenAI(
    deployment_name="gpt-4o-mini",
    temperature=0
)


# ============================================================
# 5 QUERY REWRITING
# ============================================================

rewrite_prompt = ChatPromptTemplate.from_template("""
Rewrite the question to improve document retrieval.

Question:
{question}
""")

query_rewriter = (
    rewrite_prompt
    | query_model
    | StrOutputParser()
)


# ============================================================
# 6 FORMAT DOCUMENTS
# ============================================================

format_docs = RunnableLambda(
    lambda docs: "\n".join(doc.page_content for doc in docs)
)


# ============================================================
# 7 RETRIEVAL STEP
# ============================================================

retrieval_chain = (
    query_rewriter
    | RunnableLambda(lambda q: retriever.invoke(q))
)


# ============================================================
# 8 RERANKING (Cross Encoder style scoring)
# ============================================================

rerank_prompt = ChatPromptTemplate.from_template("""
Given the question and documents, return the top 3 most relevant documents.

Question:
{question}

Documents:
{documents}
""")

reranker = (
    rerank_prompt
    | query_model
    | StrOutputParser()
)


rerank_chain = RunnableLambda(
    lambda x: {
        "question": x["question"],
        "documents": "\n".join(doc.page_content for doc in x["docs"])
    }
) | reranker


# ============================================================
# 9 RAG PROMPT
# ============================================================

rag_prompt = ChatPromptTemplate.from_template("""
Answer the question using the context.

Context:
{context}

Question:
{question}
""")


answer_chain = (
    rag_prompt
    | answer_model
    | StrOutputParser()
)


# ============================================================
# 10 SAFETY MODERATION
# ============================================================

moderation_prompt = ChatPromptTemplate.from_template("""
Check the answer for unsafe or harmful content.

Answer:
{answer}

If the answer is safe return it unchanged.
""")

moderation_chain = (
    moderation_prompt
    | moderation_model
    | StrOutputParser()
)


# ============================================================
# 11 FULL MULTI-MODEL RAG PIPELINE
# ============================================================

rag_pipeline = (

    RunnableParallel(
        docs = RunnableLambda(lambda x: x["question"]) | retrieval_chain,
        question = RunnableLambda(lambda x: x["question"])
    )

    | RunnableLambda(
        lambda x: {
            "question": x["question"],
            "docs": x["docs"]
        }
    )

    | RunnableParallel(
        context = RunnableLambda(
            lambda x: "\n".join(doc.page_content for doc in x["docs"])
        ),
        question = RunnableLambda(lambda x: x["question"])
    )

    | answer_chain

    | RunnableLambda(lambda x: {"answer": x})

    | moderation_chain
)


# ============================================================
# 12 INPUT QUESTION
# ============================================================

question = {"question": "What sales did Ram report in Pune?"}


# ============================================================
# 13 RUN PIPELINE
# ============================================================

result = rag_pipeline.invoke(question)

print("\nFINAL ANSWER:\n")
print(result)
