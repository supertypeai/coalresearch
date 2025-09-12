from langchain.chat_models import init_chat_model

from insider_news.utils.config import GROQ_API_KEY1, GROQ_API_KEY2, GROQ_API_KEY3


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
            
            model_providers = {
                "openai/gpt-oss-20b": "groq",
                "openai/gpt-oss-120b": "groq",
                "qwen/qwen3-32b": "groq",
                "deepseek-r1-distill-llama-70b": "groq",
                "llama-3.3-70b-versatile": "groq",
                "llama-3.1-8b-instant": "groq",
            }

            groq_api_keys = [GROQ_API_KEY1, GROQ_API_KEY2, GROQ_API_KEY3]

            llms= []
            for model, provider in model_providers.items():
                if provider == 'groq':
                    for groq_key in groq_api_keys:
                        llms.append(
                            init_chat_model(
                                model,
                                model_provider=provider,
                                temperature=0.2,
                                max_retries=3,
                                api_key=groq_key,
                            )
                        )
                
            cls._instance._llms = llms 

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
