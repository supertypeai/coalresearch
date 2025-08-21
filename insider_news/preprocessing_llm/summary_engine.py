from pydantic                       import Field, BaseModel
from langchain_core.output_parsers  import JsonOutputParser
from langchain.prompts              import PromptTemplate 
from langchain_core.runnables       import RunnableParallel
from operator                       import itemgetter

from .llms import LLMCollection

import json 
import time 


LLMCOLLECTION = LLMCollection()


class SummaryNews(BaseModel):
    title: str = Field(description="Title from an article")
    body: str = Field(description="Two sentences summary from an article")


def get_summary(article_content: str, article_url: str) -> str:
    summarize_template = """
        You are a mining expert journalism,  
        Your task is to generate summary based on the full article content.

        Article Content:
        {article}

        Note:
        - For the body: Provide a concise, maximum 2 sentences summary highlighting main points, key events, and mining metrics 
          (production volumes, capex, reserves, grades, shipments, smelter/plant status, permits, ESG incidents). 
        - For the title: Create a one sentence title that is not misleading and gives general understanding of the article.
          
        Ensure to return the title and summary in the following JSON format.
        {format_instructions}
    """

    # Define the output parser and prompt template
    summary_parser = JsonOutputParser(pydantic_object=SummaryNews)
    summary_prompt = PromptTemplate(
        template=summarize_template, 
        input_variables=[
            "article",
        ],
        partial_variables={
            "format_instructions": summary_parser.get_format_instructions()
        },
    )
    # Create a runnable summary system that prepares the input for the LLM
    runnable_summary_system = RunnableParallel(
        {   
            "article": itemgetter("article"),
        }
    )

    for llm in LLMCOLLECTION.get_llms():
        try:
            # Define the summary chain
            summary_chain = (
                runnable_summary_system 
                | summary_prompt
                | llm
                | summary_parser 
            )
        
            # Invoke the scoring chain with the provided article details
            summary_result = summary_chain.invoke({
                'article': article_content,
            })
            time.sleep(3)

            if not summary_result.get('title') or not summary_result.get('body'):
                print('Summary response not complete')
                continue 

            print(f'[SUCCES] Summarize for url {article_url}')
            return summary_result
            
        except json.JSONDecodeError as error: 
            print(f"Failed to parse JSON responsee {error}")
            continue
        
        except Exception as error:
            print(f"[Summary] LLM failed with error: {error}")
            continue 

    

