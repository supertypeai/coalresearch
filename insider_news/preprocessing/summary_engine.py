from langchain_core.output_parsers import JsonOutputParser
from langchain.prompts import PromptTemplate 
from langchain_core.runnables import RunnableParallel
from operator import itemgetter

from insider_news.llm.client import LLMCollection
from insider_news.llm.prompts import PromptsCollections, SummaryNews

import json 
import time 
import logging 


LOGGER = logging.getLogger(__name__) 


def get_summary(article_content: str) -> dict:
    llm_collection = LLMCollection()

    summarize_template = PromptsCollections.get_summary_prompts()

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
    
    runnable_summary_system = RunnableParallel(
        {   
            "article": itemgetter("article"),
        }
    )

    for llm in llm_collection.get_llms():
        try:
            llm_used = getattr(llm, 'model_name', getattr(llm, 'model', 'unknown'))
            LOGGER.info(f'LLM used: {llm_used}')

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
            time.sleep(10)
            
            title = summary_result.get('title')
            body = summary_result.get('body')

            if not title or not body:
                LOGGER.info('Summary response not complete, trying next llm')
                continue 

            return title, body
            
        except json.JSONDecodeError as error: 
            LOGGER.error(f"Failed to parse JSON responsee {error}")
            continue
        
        except Exception as error:
            LOGGER.error(f"[Summary] LLM failed with error: {error}")
            continue 
    
    LOGGER.error("All llm failed return None for summary")
    return None 
    
