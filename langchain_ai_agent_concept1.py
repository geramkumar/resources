# ==========================================================
# LangChain AI Agent Example
# Example : Multi-tool assistant
# Tools:
# 1. Weather tool
# 2. Calculator tool
# 3. Capital lookup tool
# 4. Word counter tool
# ==========================================================

# pip install langchain langchain-openai

from langchain_openai import AzureChatOpenAI
from langchain.tools import tool
from langchain.agents import initialize_agent
from langchain.agents import AgentType


# ==========================================================
# 1. Azure OpenAI Configuration
# ==========================================================

AZURE_ENDPOINT = "YOUR_ENDPOINT"
AZURE_API_KEY = "YOUR_KEY"
API_VERSION = "2024-02-15-preview"

CHAT_DEPLOYMENT = "gpt-4o-mini"


# ==========================================================
# 2. Tool 1 : Weather Tool
# ==========================================================

@tool
def get_weather(city: str) -> str:
    """Returns weather information for a city"""

    weather_data = {
        "Pune": "28°C, Sunny",
        "Mumbai": "30°C, Humid",
        "Bangalore": "25°C, Cloudy"
    }

    print(f"\n[TOOL CALLED] get_weather → {city}")

    return weather_data.get(city, "Weather data not available")


# ==========================================================
# 3. Tool 2 : Calculator Tool
# ==========================================================

@tool
def calculator(expression: str) -> str:
    """Evaluates a math expression"""

    print(f"\n[TOOL CALLED] calculator → {expression}")

    result = eval(expression)

    return f"Result is {result}"


# ==========================================================
# 4. Tool 3 : Capital Lookup Tool
# ==========================================================

@tool
def get_capital(country: str) -> str:
    """Returns the capital city of a country"""

    capitals = {
        "India": "New Delhi",
        "USA": "Washington DC",
        "France": "Paris",
        "Japan": "Tokyo"
    }

    print(f"\n[TOOL CALLED] get_capital → {country}")

    return capitals.get(country, "Capital not found")


# ==========================================================
# 5. Tool 4 : Word Counter
# ==========================================================

@tool
def word_counter(text: str) -> str:
    """Counts number of words in text"""

    print(f"\n[TOOL CALLED] word_counter")

    count = len(text.split())

    return f"Word count is {count}"


tools = [
    get_weather,
    calculator,
    get_capital,
    word_counter
]


print("\n======================")
print("TOOLS REGISTERED")
print("======================")

for t in tools:
    print("-", t.name)


# ==========================================================
# 6. Initialize LLM
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
# 7. Create Agent
# ==========================================================

agent = initialize_agent(
    tools,
    llm,
    agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
    verbose=True
)

print("\nAgent created")


# ==========================================================
# 8. Example Queries
# ==========================================================

questions = [

    "What is the weather in Pune?",

    "What is 45 * 2?",

    "What is the capital of Japan?",

    "Count words in: LangChain agents are powerful AI systems"
]


for q in questions:

    print("\n======================")
    print("USER QUESTION")
    print("======================")
    print(q)

    response = agent.run(q)

    print("\n======================")
    print("FINAL ANSWER")
    print("======================")
    print(response)
