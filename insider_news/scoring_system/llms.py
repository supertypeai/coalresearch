from langchain_openai import ChatOpenAI

from dotenv import load_dotenv
import os 

load_dotenv(override=True)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

class LLMModels:
    """
    Define LLM models with OpenAI
    """
    def __init__(self):
        self.scoring_model = self.create_scoring_ai()

    # Define a function for intialize OpenAI LLM
    def create_scoring_ai(self):
        llm_model = ChatOpenAI(
            temperature=1,
            max_tokens=1024*4,
            model="gpt-4o",
            api_key=OPENAI_API_KEY, 
        )

        return llm_model
