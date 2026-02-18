pip install langchain langchain-community langchain-openai chromadb tiktoken
###
AZURE_OPENAI_API_KEY=xxxxxxxx
AZURE_OPENAI_ENDPOINT=https://<your-resource-name>.openai.azure.com/
AZURE_OPENAI_API_VERSION=2024-02-15-preview

####
import os

from langchain_community.document_loaders import TextLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_openai import AzureOpenAIEmbeddings, AzureChatOpenAI
from langchain_community.vectorstores import Chroma
from langchain.chains import create_retrieval_chain
from langchain.chains.combine_documents import create_stuff_documents_chain
from langchain.prompts import ChatPromptTemplate

# ---------------------------
# 1. Load Document
# ---------------------------
loader = TextLoader("data.txt", encoding="utf-8")
documents = loader.load()

# ---------------------------
# 2. Split into Chunks
# ---------------------------
text_splitter = RecursiveCharacterTextSplitter(
    chunk_size=500,
    chunk_overlap=100
)

chunks = text_splitter.split_documents(documents)

# ---------------------------
# 3. Azure OpenAI Embeddings
# ---------------------------
#
#LangChain first check if azure_endpoint and api_key are explicitly mentioned in AzureOpenAIEmbeddings, AzureChatOpenAI. If not it will read from environment variables
#azure_endpoint = (provided_azure_endpoint or os.getenv("AZURE_OPENAI_ENDPOINT"))
#api_key = (provided_api_key or os.getenv("AZURE_OPENAI_API_KEY"))

##The Azure OpenAI URL is NOT passed in code because LangChain reads it implicitly from environment variables and builds the HTTPS request internally.
#AzureOpenAIEmbeddings Read AZURE_OPENAI_ENDPOINT from .env
#AzureOpenAIEmbeddings Read AZURE_OPENAI_API_KEY from .env
#AzureOpenAIEmbeddings Read deployment name
#AzureOpenAIEmbeddings Construct REST URL
#AzureOpenAIEmbeddings Call Azure OpenAI Embedding API
#Langchain internally constructs POST https://<resource-name>.openai.azure.com/openai/deployments/text-embedding-3-small/embeddings?api-version=2024-02-15-preview

embeddings = AzureOpenAIEmbeddings(
    azure_deployment="text-embedding-3-small",  # embedding deployment name
    api_version=os.getenv("AZURE_OPENAI_API_VERSION")
)

# ---------------------------
# 4. Store in ChromaDB
# ---------------------------
vectorstore = Chroma.from_documents(
    documents=chunks,
    embedding=embeddings,
    persist_directory="./chroma_db"
)

#below vectorstore.similarity_search command can't be considered as RAG. 
#because the below command calls only the embedding model and uses similarity search algorithms
#such as cosine similarity. LLMs are not in scope.
#below doesn't do : Augmentation (prompt stuffing), Generation (LLM answer), Reasoning,Hallucination control, End-to-end RAG

#Cromadb uses L2 distance (Euclidean distance) algorithm.
#Here loweer the score is (ex: 0.01243) the content is more similar. 0 is very good. range can be 1 or 2 or higher
#but in Cosine similarity, range is between 1-1 to 1. 1 -> exact match, 0 -> No match, -1 -> opposite

query = "What is python?"
similar_docs = vectorstore.similarity_search_with_score(query, k=3)
similar_docs

retriever = vectorstore.as_retriever(
    search_type="similarity",
    search_kwargs={"k": 3}
)

# ---------------------------
# 5. Azure OpenAI LLM
# ---------------------------
llm = AzureChatOpenAI(
    azure_deployment="gpt-4o-mini",   # chat model deployment name
    api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
    temperature=0,
    max_tokens=500 #totatl tokens it can generate
)

#other ways to create llm
#from langchain.chat_models.base import init_chat_model
#llm=init_chat_model("groq:")
#llm=init_chat_model("openai:gpt-3.5-turbo")

res = llm.invoke("What is Agentic AI")
print(res)

# ---------------------------
# 6. Prompt Template
# ---------------------------
prompt = ChatPromptTemplate.from_template("""
Answer the question strictly using the provided context.
If the answer is not in the context, say "I don't know".

Context:
{context}

Question:
{input}
""")

# ---------------------------
# 7. Stuffed Document Chain
# ---------------------------
document_chain = create_stuff_documents_chain(
    llm=llm,
    prompt=prompt
)

# ---------------------------
# 8. RAG Retrieval Chain
# ---------------------------
rag_chain = create_retrieval_chain(
    retriever=retriever,
    combine_docs_chain=document_chain
)

# ---------------------------
# 9. Query
# ---------------------------
query = "What is explained in the document?"
response = rag_chain.invoke({"input": query})

print("\n--- ANSWER ---")
print(response["answer"])





### Same is refactored into Enterprise grade solution
# unzip azure_openai_rag_enterprise.zip
# cd azure-openai-rag-enterprise
# uv sync
# python src/main.py
