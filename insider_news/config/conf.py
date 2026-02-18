from dotenv import load_dotenv

import os 


load_dotenv(override=True)


OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
GROQ_API_KEY1 = os.getenv("GROQ_API_KEY1")
GROQ_API_KEY_DEV = os.getenv("GROQ_API_KEY_DEV")
GROQ_API_KEY2 = os.getenv("GROQ_API_KEY2")
GROQ_API_KEY3 = os.getenv("GROQ_API_KEY3")
PROXY = os.getenv("PROXY")
