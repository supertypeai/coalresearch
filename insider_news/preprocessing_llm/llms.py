from langchain.chat_models import init_chat_model

from dotenv import load_dotenv
import os 


load_dotenv(override=True)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
GROQ_API_KEY1 = os.getenv("GROQ_API_KEY1")
GROQ_API_KEY2 = os.getenv("GROQ_API_KEY2")
GROQ_API_KEY3 = os.getenv("GROQ_API_KEY3")


class LLMCollection:
    """
    @brief Singleton class to manage a collection of LLM (Large Language Model) instances.
    This class ensures that only one instance of the LLMCollection exists and provides methods to add and retrieve LLM instances.
    """
    _instance = None

    def __new__(cls):
        """
        @brief Creates a new instance of LLMCollection if it doesn't already exist.
        @return The singleton instance of LLMCollection.
        """
        if cls._instance is None:
            cls._instance = super(LLMCollection, cls).__new__(cls)
            cls._instance._llms = [
                init_chat_model(
                    "llama3-70b-8192",
                    model_provider="groq",
                    temperature=0.2,
                    max_retries=3,
                    api_key=GROQ_API_KEY1
                ),
                init_chat_model(
                   "llama-3.3-70b-versatile",
                    model_provider="groq",
                    temperature=0.2,
                    max_retries=3,
                    api_key=GROQ_API_KEY1
                ),
                init_chat_model(
                    "llama3-70b-8192",
                    model_provider="groq",
                    temperature=0.2,
                    max_retries=3,
                    api_key=GROQ_API_KEY2
                ),
                init_chat_model(
                   "llama-3.3-70b-versatile",
                    model_provider="groq",
                    temperature=0.2,
                    max_retries=3,
                    api_key=GROQ_API_KEY2
                ), 
                init_chat_model(
                    "llama3-70b-8192",
                    model_provider="groq",
                    temperature=0.2,
                    max_retries=3,
                    api_key=GROQ_API_KEY3
                ),
                init_chat_model(
                   "llama-3.3-70b-versatile",
                    model_provider="groq",
                    temperature=0.2,
                    max_retries=3,
                    api_key=GROQ_API_KEY3
                ), 
                init_chat_model(
                   "gpt-4.1-mini",
                    model_provider="openai",
                    temperature=0.2,
                    max_retries=3,
                    api_key=OPENAI_API_KEY
                ), 
            ]
        return cls._instance

    def add_llm(self, llm):
        """
        @brief Adds a new LLM instance to the collection.
        @param llm The LLM instance to be added to the collection.
        """
        self._llms.append(llm)

    def get_llms(self):
        """
        @brief Retrieves the list of LLM instances in the collection.
        @return A list of LLM instances.
        """
        return self._llms
