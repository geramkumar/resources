from langchain_community.document_loaders import TextLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_chroma import Chroma
from langchain_openai import OpenAIEmbeddings
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser

OPENAI_API_KEY = "<key>"

loader = TextLoader("../data/dataengineering.txt", encoding="utf-8")
documents = loader.load()
documents


splitter = RecursiveCharacterTextSplitter(
    chunk_size=100,
    chunk_overlap=20
)
chunks = splitter.split_documents(documents)
chunks_str = [chunk.page_content for chunk in chunks]

embedding = OpenAIEmbeddings(model="text-embedding-3-small", api_key=OPENAI_API_KEY)
embedded_chunks = embedding.embed_documents(chunks_str)
embedded_chunks

vectorstore = Chroma.from_documents(
    documents=chunks,
    embedding=embedding,
    persist_directory="./chroma_db"
)
results = vectorstore.similarity_search_with_score("What is data engineering?", k=3)
for i in results:
    print(i)

retriever = vectorstore.as_retriever(
    search_type="similarity",
    search_kwargs={"k": 3}
)


def format_docs(docs):
    return "\n\n".join(doc.page_content for doc in docs)

question = "why is pyspark?"

prompt = ChatPromptTemplate.from_template("""
You are a helpful assistant.
Answer the question using ONLY the context below.

Context:
{context}

Question:
{question}
""")

llm = ChatOpenAI(
    model="gpt-4.1-mini",
    api_key=OPENAI_API_KEY,
    temperature=0
)


rag_chain = (
    {
        "context": retriever | format_docs,
        "question": RunnablePassthrough()
    }
    | prompt
    | llm
    | StrOutputParser()
)

response = rag_chain.invoke(question)
print(response)


