runnable execution methods:

.invoke() → run one input
.batch() → run multiple inputs at once
.stream() → stream tokens as the LLM generates them

Input: 1️⃣ .invoke() — Single Execution
What sales did Ram report in Pune?

Input: 2️⃣ .batch() — Multiple Inputs - Runs many inputs at once (parallel execution).
1 What sales did Ram report in Pune?
2 What does LangChain support?

3️⃣ .stream() — Streaming Output
Instead of waiting for the full response:
Ram sold 200 units in Pune during Jan 2026,
180 units in Feb 2026,
and 220 units in Mar 2026.

# ============================================================
# END-TO-END RAG TO DEMONSTRATE RUNNABLE METHODS
# invoke() | batch() | stream()
# LangChain v1+
# ============================================================

# pip install langchain langchain-openai faiss-cpu


import random
from langchain_openai import AzureChatOpenAI, AzureOpenAIEmbeddings
from langchain.vectorstores import FAISS
from langchain.schema import Document
from langchain.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser


# ============================================================
# 1 DATASET
# ============================================================

langchain_docs = [
"LangChain is a framework for building LLM powered applications.",
"LangChain supports retrieval augmented generation pipelines.",
"LangChain integrates with vector databases like FAISS, Chroma and Pinecone.",
"LangChain provides tools for prompt engineering and chaining.",
"LangChain agents allow LLMs to interact with external tools.",
"LangChain supports structured output using Pydantic and TypedDict.",
"LangChain provides memory modules for chat applications."
]

sales_docs = [
"Ram sold 200 units in Pune during Jan 2026.",
"Ram sold 180 units in Pune during Feb 2026.",
"Ram sold 220 units in Pune during Mar 2026.",
"Anita sold 300 units in Bangalore during Jan 2026.",
"Shyam sold 150 units in Mumbai during Jan 2026."
]

docs = langchain_docs + sales_docs
random.shuffle(docs)

documents = [Document(page_content=d) for d in docs]


# ============================================================
# 2 VECTOR STORE
# ============================================================

embeddings = AzureOpenAIEmbeddings(
    model="text-embedding-3-small"
)

vectorstore = FAISS.from_documents(documents, embeddings)

retriever = vectorstore.as_retriever(search_kwargs={"k":3})


# ============================================================
# 3 LLM
# ============================================================

llm = AzureChatOpenAI(
    deployment_name="gpt-4o-mini",
    temperature=0
)


# ============================================================
# 4 PROMPT
# ============================================================

rag_prompt = ChatPromptTemplate.from_template("""
Answer the question using the context.

Context:
{context}

Question:
{question}
""")


# ============================================================
# 5 FORMAT DOCUMENTS
# ============================================================

def format_docs(docs):
    return "\n".join(doc.page_content for doc in docs)


# ============================================================
# 6 LCEL RAG CHAIN (Runnable Pipeline)
# ============================================================

rag_chain = (
    {
        "context": lambda x: format_docs(retriever.invoke(x["question"])),
        "question": lambda x: x["question"]
    }
    | rag_prompt
    | llm
    | StrOutputParser()
)


# ============================================================
# 7 RUNNABLE METHOD 1 : invoke()
# ============================================================

print("\n============================")
print("RUNNABLE METHOD : invoke()")
print("============================")

question = {"question": "What sales did Ram report in Pune?"}

result = rag_chain.invoke(question)

print(result)


# ============================================================
# 8 RUNNABLE METHOD 2 : batch()
# ============================================================

print("\n============================")
print("RUNNABLE METHOD : batch()")
print("============================")

questions = [
    {"question": "What sales did Ram report in Pune?"},
    {"question": "What does LangChain support?"}
]

results = rag_chain.batch(questions)

for r in results:
    print("\nAnswer:", r)


# ============================================================
# 9 RUNNABLE METHOD 3 : stream()
# ============================================================

print("\n============================")
print("RUNNABLE METHOD : stream()")
print("============================")

for chunk in rag_chain.stream(
    {"question": "What sales did Ram report in Pune?"}
):
    print(chunk, end="", flush=True)

print("\n")
