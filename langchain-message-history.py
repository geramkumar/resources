'''
Notes:

RunnableWithMessageHistory is a LangChain tool that acts as a "memory manager" for chatbots. 
RunnableWithMessageHistory
       |
       |---- load history
       |
       |---- run chain
       |
       |---- save messages

Workflow:
========
User Input
     ↓
RunnableWithMessageHistory
     ↓
get_history(session_id)
     ↓
Inject history into prompt
     ↓
Run chain (prompt → llm → parser)
     ↓
Save Human + AI messages


chat_chain = RunnableWithMessageHistory(
   memory_chain,
    # STEP 1 — RETRIEVE HISTORY OBJECT
    # LangChain will call this before executing the chain
    lambda session_id: store.setdefault(session_id, ChatMessageHistory()),
    input_messages_key="question",
    # STEP 2 — HISTORY WILL BE INSERTED INTO THIS PROMPT VARIABLE
    history_messages_key="history"
)

IMPORTANT RunnableWithMessageHistory implictly performs the following:
- add the user question in history and add llm response (AImessage) in history

history = get_history(session_id)
history.add_user_message(user_input)
response = chain.invoke(...)
history.add_ai_message(response)

'''

#####################################################################
# 1. IMPORT LIBRARIES
#####################################################################

from langchain_openai import AzureChatOpenAI, AzureOpenAIEmbeddings
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables.history import RunnableWithMessageHistory
from langchain_core.runnables import RunnableLambda
from langchain.memory import ChatMessageHistory
from langchain_community.vectorstores import FAISS
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_community.document_loaders import TextLoader


#####################################################################
# 2. AZURE OPENAI MODEL CONFIGURATION
#####################################################################

llm = AzureChatOpenAI(
    azure_deployment="gpt-4",
    api_version="2024-02-15-preview",
    temperature=0
)

embeddings = AzureOpenAIEmbeddings(
    azure_deployment="text-embedding-3-large"
)


#####################################################################
# 3. SAMPLE DATASET FOR RAG
#####################################################################

text = """
LangChain is a framework used to build applications powered by LLMs.

Retrieval Augmented Generation (RAG) improves LLM answers by retrieving
relevant documents from a knowledge base.

Conversational memory allows AI systems to remember previous
interactions in a conversation.
"""

with open("rag_data.txt","w") as f:
    f.write(text)


#####################################################################
# 4. LOAD DATA AND BUILD VECTOR DATABASE
#####################################################################

loader = TextLoader("rag_data.txt")
docs = loader.load()

splitter = RecursiveCharacterTextSplitter(chunk_size=120, chunk_overlap=20)
chunks = splitter.split_documents(docs)

vectorstore = FAISS.from_documents(chunks, embeddings)

retriever = vectorstore.as_retriever()


#####################################################################
# 5. PROMPT TEMPLATE
#####################################################################

prompt = ChatPromptTemplate.from_messages([
    ("system","You are a helpful AI assistant"),
    ("placeholder","{history}"),
    ("human","{question}")
])


#####################################################################
# 6. BASELINE CHAT (NO MEMORY)
#####################################################################

"""
PURPOSE
-------

This section demonstrates how an LLM behaves WITHOUT any conversational
memory.

Each request to the LLM is independent. The model does not remember
previous questions unless the history is manually provided.

REAL LIFE USE CASES
-------------------

1. Stateless APIs
   Example:
   Simple REST API that answers one question at a time.

2. Search style systems
   Example:
   "Ask a question → get answer → conversation ends"

3. High throughput systems
   Example:
   Large scale Q&A systems where memory is unnecessary.

LIMITATION
----------

Follow-up questions fail because the LLM has no awareness of
previous interactions.
"""

basic_chain = prompt | llm | StrOutputParser()

input_1 = {
    "history": [],
    "question": "What is LangChain?"
}

response_1 = basic_chain.invoke(input_1)

print("\nNO MEMORY - RESPONSE 1")
print(response_1)


input_2 = {
    "history": [],
    "question": "What did I ask earlier?"
}

response_2 = basic_chain.invoke(input_2)

print("\nNO MEMORY - RESPONSE 2")
print(response_2)



#####################################################################
# 7. MESSAGE HISTORY STORE
#####################################################################

"""
PURPOSE
-------

RunnableWithMessageHistory requires a storage system where the
conversation messages are stored.

Here we use a simple Python dictionary.

In real systems this can be replaced with:

Redis
PostgreSQL
MongoDB
DynamoDB
"""

store = {}



#####################################################################
# 8. CONVERSATION BUFFER MEMORY
#####################################################################

"""
PURPOSE
-------

This is the most common conversational memory pattern.

RunnableWithMessageHistory automatically:

1. Retrieves previous conversation messages
2. Injects them into the prompt
3. Stores new messages after the response

This replicates the behavior of the old:

ConversationBufferMemory (LangChain v0)

REAL LIFE USE CASES
-------------------

Chatbots
Customer support bots
AI copilots
Coding assistants
Interactive tutoring systems

EXAMPLE
-------

User: What is LangChain?
AI: explanation

User: What did I ask earlier?
AI: You asked about LangChain.

This works because the conversation history is preserved.
"""

memory_chain = prompt | llm | StrOutputParser()

chat_chain = RunnableWithMessageHistory(
    memory_chain,
    lambda session_id: store.setdefault(session_id, ChatMessageHistory()),
    input_messages_key="question",
    history_messages_key="history"
)

memory_input_1 = {
    "question": "What is LangChain?"
}

memory_response_1 = chat_chain.invoke(
    memory_input_1,
    config={"configurable":{"session_id":"user1"}}
)

print("\nBUFFER MEMORY - RESPONSE 1")
print(memory_response_1)


memory_input_2 = {
    "question": "What did I ask earlier?"
}

memory_response_2 = chat_chain.invoke(
    memory_input_2,
    config={"configurable":{"session_id":"user1"}}
)

print("\nBUFFER MEMORY - RESPONSE 2")
print(memory_response_2)



#####################################################################
# 9. CONVERSATIONAL RAG MEMORY
#####################################################################

"""
PURPOSE
-------

This section combines:

Conversation Memory + Retrieval Augmented Generation

The chatbot can:

1. Remember previous conversation
2. Retrieve external knowledge
3. Generate context-aware responses

REAL LIFE USE CASES
-------------------

Enterprise knowledge assistants
Company policy chatbots
Internal documentation search
Customer support knowledge bots

EXAMPLE
-------

User: What is RAG?
AI: explanation from documents

User: Explain again briefly
AI: short explanation referencing previous conversation

This works because both:

conversation history
retrieved documents

are used by the prompt.
"""

rag_prompt = ChatPromptTemplate.from_messages([
    ("system","Answer using the provided context"),
    ("placeholder","{history}"),
    ("human","Context:\n{context}\n\nQuestion:{question}")
])

rag_chain = (
{
"context": retriever | RunnableLambda(lambda docs: "\n".join([d.page_content for d in docs])),
"question": RunnableLambda(lambda x: x["question"]),
"history": RunnableLambda(lambda x: x["history"])
}
| rag_prompt
| llm
| StrOutputParser()
)

rag_chat_chain = RunnableWithMessageHistory(
    rag_chain,
    lambda session_id: store.setdefault(session_id, ChatMessageHistory()),
    input_messages_key="question",
    history_messages_key="history"
)

rag_input_1 = {
    "question": "What is RAG?"
}

rag_response_1 = rag_chat_chain.invoke(
    rag_input_1,
    config={"configurable":{"session_id":"user2"}}
)

print("\nCONVERSATIONAL RAG - RESPONSE 1")
print(rag_response_1)


rag_input_2 = {
    "question": "Explain again briefly"
}

rag_response_2 = rag_chat_chain.invoke(
    rag_input_2,
    config={"configurable":{"session_id":"user2"}}
)

print("\nCONVERSATIONAL RAG - RESPONSE 2")
print(rag_response_2)



#####################################################################
# 10. WINDOW MEMORY (LIMITED HISTORY)
#####################################################################

"""
PURPOSE
-------

Window memory keeps only the last N messages.

This prevents the prompt from growing indefinitely.

Without windowing, long conversations can:

increase token cost
slow down responses
exceed model context limits

REAL LIFE USE CASES
-------------------

Customer support bots
Long running chat sessions
Voice assistants
AI copilots

Example window size = 2

Conversation history retained:

Last 2 messages only
"""

window_chain = RunnableWithMessageHistory(
    memory_chain,
    lambda session_id: (
        store.setdefault(session_id, ChatMessageHistory()),
        store[session_id].messages.__setitem__(
            slice(None),
            store[session_id].messages[-2:]
        ),
        store[session_id]
    )[-1],
    input_messages_key="question",
    history_messages_key="history"
)

window_input = {
    "question": "What is LangChain?"
}

window_response = window_chain.invoke(
    window_input,
    config={"configurable":{"session_id":"user3"}}
)

print("\nWINDOW MEMORY RESPONSE")
print(window_response)



#####################################################################
# 11. MULTI USER SESSION MEMORY
#####################################################################

"""
PURPOSE
-------

Each conversation session is isolated using session_id.

This allows multiple users to interact with the system
simultaneously without sharing conversation history.

REAL LIFE USE CASES
-------------------

ChatGPT style applications
Customer service platforms
Multi user AI assistants
Enterprise chat systems
"""

multi_user_input = {
    "question": "My name is Ram"
}

multi_user_response_1 = chat_chain.invoke(
    multi_user_input,
    config={"configurable":{"session_id":"userA"}}
)

print("\nMULTI USER RESPONSE 1")
print(multi_user_response_1)


multi_user_input_2 = {
    "question": "What is my name?"
}

multi_user_response_2 = chat_chain.invoke(
    multi_user_input_2,
    config={"configurable":{"session_id":"userA"}}
)

print("\nMULTI USER RESPONSE 2")
print(multi_user_response_2)
