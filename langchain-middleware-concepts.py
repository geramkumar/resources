## Interesting - Runnable lambda can be directly invoked
from langchain_core.runnables import RunnableLambda
clean_text = RunnableLambda(lambda x: x.lower())
result = clean_text.invoke("HELLO WORLD")
print(result)



### Concept
A lambda is a small anonymous function in Python that is written in one line without a name.
Example:
numbers = [1,2,3,4]
result = list(map(lambda x: x*2, numbers))


In LangChain v1+, everything works using Runnable pipelines.
Runnable = something that can run inside a chain
Example Runnable Chain: Each component below is a Runnable.
prompt | llm | parser

A Runnable is an object that can be executed with:
.invoke()
.batch()
.stream()
Example pipeline: Prompt → LLM → Output Parser

2. What is RunnableLambda in Simple Words?
RunnableLambda = convert a lambda function into a Runnable.
RunnableLambda converts a normal Python function into a LangChain Runnable so it can be used inside chains.
RunnableLambda = wrapper that allows normal Python code to run inside LangChain pipelines
LangChain pipelines only accept Runnable objects.
Normal functions cannot be directly inserted into the chain.
ex (not allowed):lambda x: x.lower() | llm
ex (allowed): Now it becomes a Runnable.
from langchain_core.runnables import RunnableLambda
clean_text = RunnableLambda(lambda x: x.lower())
result = clean_text.invoke("HELLO WORLD")
print(result)

###  Example Without RunnableLambda => Suppose we have a function:

def format_docs(docs):
    return "\n".join(doc.page_content for doc in docs)

If we try: retriever | format_docs | llm
It will fail because: format_docs is not a Runnable

### Example Using RunnableLambda
from langchain_core.runnables import RunnableLambda
format_docs = RunnableLambda(
    lambda docs: "\n".join(doc.page_content for doc in docs)
)

Now we can use it in pipeline: chain = retriever | format_docs | llm
Execution:
chain.invoke("What is LangChain?")

###############
1. What is Middleware in RAG? (Simple Explanation)
Middleware is a small processing step inserted between stages of a RAG pipeline to modify, validate, filter, or log the data.
In simple words: Middleware = logic that runs in between steps of the pipeline.

2. What is RunnableLambda in Simple Words?
RunnableLambda allows you to run a custom Python function inside a LangChain pipeline.
In simple words: RunnableLambda = a wrapper that lets LangChain run your Python logic inside the chain.

Without RunnableLambda

Normal Python function:
def clean_question(x):
    return x.lower()

LangChain cannot directly use this in LCEL pipelines.

With RunnableLambda
from langchain_core.runnables import RunnableLambda
clean_question = RunnableLambda(lambda x: x.lower())

Now it can be used inside the pipeline:
clean_question | retriever | llm


  
Normal RAG Pipeline:
  
User Question
      ↓
Retriever
      ↓
Prompt
      ↓
LLM
      ↓
Answer

Middleware RAG Pipeline:
  
User Question
      ↓
Middleware (rewrite query)
      ↓
Retriever
      ↓
Middleware (filter docs)
      ↓
Prompt
      ↓
LLM
      ↓
Middleware (moderate output)
      ↓
Answer
  

Example Execution

Input
Tell me Ram sales in Pune

Query Rewriting
What sales did Ram report in Pune?

Retrieved Context
Ram sold 200 units in Pune during Jan 2026.
Ram sold 180 units in Pune during Feb 2026.
Ram sold 220 units in Pune during Mar 2026.

Output
Ram sold 200 units in Pune during Jan 2026,
180 units in Feb 2026,
and 220 units in Mar 2026.

Where Each Middleware Is Applied
Middleware	Role
Safety Filter	Blocks unsafe user prompts
Query Rewriting	Improves search query
Logging	Tracks rewritten query
Retrieval Filtering	Removes irrelevant docs
Reranking	Sorts docs by relevance
Response Moderation	Cleans unsafe outputs


# ============================================================
# PRODUCTION STYLE RAG WITH MIDDLEWARE
# LangChain v1+
# ============================================================

# pip install langchain langchain-openai faiss-cpu


import random
from langchain_openai import AzureChatOpenAI, AzureOpenAIEmbeddings
from langchain.vectorstores import FAISS
from langchain.schema import Document
from langchain.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnableLambda


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
"LangChain provides memory modules for chat applications."
]

sales_docs = [
"Ram sold 200 units in Pune during Jan 2026.",
"Ram sold 180 units in Pune during Feb 2026.",
"Ram sold 220 units in Pune during Mar 2026.",
"Anita sold 300 units in Bangalore during Jan 2026.",
"Shyam sold 150 units in Mumbai during Jan 2026.",
"Ravi sold 250 units in Hyderabad during Jan 2026."
]

docs = langchain_docs + sales_docs
random.shuffle(docs)

documents = [Document(page_content=d) for d in docs]


# ============================================================
# VECTOR STORE
# ============================================================

embeddings = AzureOpenAIEmbeddings(model="text-embedding-3-small")

vectorstore = FAISS.from_documents(documents, embeddings)

retriever = vectorstore.as_retriever(search_kwargs={"k":5})


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

question = "Tell me Ram sales in Pune"

print("\nUser Question:", question)


# ============================================================
# MIDDLEWARE 1 : SAFETY FILTER
# ============================================================

safety_filter = RunnableLambda(
    lambda x: x if "hack" not in x["question"].lower()
    else {"question": "Blocked unsafe query"}
)


# ============================================================
# MIDDLEWARE 2 : QUERY REWRITING
# ============================================================

rewrite_prompt = ChatPromptTemplate.from_template("""
Rewrite the question to improve document retrieval.

Question:
{question}
""")

query_rewriter = (
    rewrite_prompt
    | llm
    | StrOutputParser()
)


# ============================================================
# MIDDLEWARE 3 : LOGGING
# ============================================================

logger = RunnableLambda(
    lambda x: (print("\nRewritten Query:", x), x)[1]
)


# ============================================================
# RETRIEVE DOCUMENTS
# ============================================================

retrieval = RunnableLambda(lambda q: retriever.invoke(q))


# ============================================================
# MIDDLEWARE 4 : RETRIEVAL FILTERING
# Remove irrelevant docs
# ============================================================

retrieval_filter = RunnableLambda(
    lambda docs: [d for d in docs if "sold" in d.page_content.lower()]
)


# ============================================================
# MIDDLEWARE 5 : RERANKING
# Simple scoring based on keyword match
# ============================================================

reranker = RunnableLambda(
    lambda docs: sorted(
        docs,
        key=lambda d: d.page_content.count("Ram"),
        reverse=True
    )[:3]
)


# ============================================================
# FORMAT CONTEXT
# ============================================================

format_docs = RunnableLambda(
    lambda docs: "\n".join(d.page_content for d in docs)
)


# ============================================================
# RAG PROMPT
# ============================================================

rag_prompt = ChatPromptTemplate.from_template("""
Answer the question using the context.

Context:
{context}

Question:
{question}
""")


# ============================================================
# RESPONSE MODERATION
# ============================================================

response_moderation = RunnableLambda(
    lambda x: x.replace("hack", "[filtered]")
)


# ============================================================
# LCEL FINAL CHAIN
# ============================================================

rag_chain = (

    safety_filter

    | {
        "question":
            RunnableLambda(lambda x: x["question"])
            | query_rewriter
            | logger,

        "context":
            RunnableLambda(lambda x: x["question"])
            | query_rewriter
            | retrieval
            | retrieval_filter
            | reranker
            | format_docs
    }

    | rag_prompt
    | llm
    | StrOutputParser()
    | response_moderation
)


# ============================================================
# RUN PIPELINE
# ============================================================

result = rag_chain.invoke({"question": question})

print("\nFinal Answer:\n")
print(result)
