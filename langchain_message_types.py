# ==========================================================
# LangChain Message Types Demo
# Demonstrates:
# 1. SystemMessage
# 2. HumanMessage
# 3. AIMessage
# 4. ToolMessage
# ==========================================================

# pip install langchain langchain-openai

from langchain_openai import AzureChatOpenAI
from langchain_core.messages import (
    SystemMessage,
    HumanMessage,
    AIMessage,
    ToolMessage
)

# ==========================================================
# 1. Azure OpenAI Configuration
# ==========================================================

AZURE_ENDPOINT = "YOUR_ENDPOINT"
AZURE_API_KEY = "YOUR_KEY"
API_VERSION = "2024-02-15-preview"

CHAT_DEPLOYMENT = "gpt-4o-mini"


# ==========================================================
# 2. Initialize Chat Model
# ==========================================================

llm = AzureChatOpenAI(
    azure_endpoint=AZURE_ENDPOINT,
    api_key=AZURE_API_KEY,
    api_version=API_VERSION,
    deployment_name=CHAT_DEPLOYMENT,
    temperature=0
)

print("\nLLM initialized")


# ==========================================================
# 3. Create Messages
# ==========================================================

system_msg = SystemMessage(
    content="You are a helpful assistant that explains concepts simply."
)

human_msg = HumanMessage(
    content="What is LangChain?"
)

print("\n========================")
print("SYSTEM MESSAGE")
print("========================")
print(system_msg)

print("\n========================")
print("HUMAN MESSAGE")
print("========================")
print(human_msg)


# ==========================================================
# 4. Send Messages to LLM
# ==========================================================

messages = [system_msg, human_msg]

response = llm.invoke(messages)

print("\n========================")
print("AI RESPONSE")
print("========================")

print(response)

print("\nAI Message Content:")
print(response.content)


# ==========================================================
# 5. Simulate Conversation History
# ==========================================================

ai_msg = AIMessage(content=response.content)

human_msg2 = HumanMessage(
    content="Explain it in one sentence."
)

conversation = [
    system_msg,
    human_msg,
    ai_msg,
    human_msg2
]

print("\n========================")
print("FULL CONVERSATION")
print("========================")

for m in conversation:
    print(type(m).__name__, ":", m.content)


response2 = llm.invoke(conversation)

print("\n========================")
print("AI RESPONSE (SECOND TURN)")
print("========================")
print(response2.content)


# ==========================================================
# 6. Example Tool Message
# ==========================================================

tool_msg = ToolMessage(
    content="Temperature in Pune is 28°C",
    tool_call_id="weather_tool"
)

print("\n========================")
print("TOOL MESSAGE EXAMPLE")
print("========================")

print(tool_msg)
