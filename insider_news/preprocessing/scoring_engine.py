from langchain_core.output_parsers import JsonOutputParser
from langchain.prompts import ChatPromptTemplate

from insider_news.llm.client import LLMCollection
from insider_news.llm.prompts import PromptsCollections, ScoringNews

import json 
import time 
import logging 


LOGGER = logging.getLogger(__name__) 


def get_scoring_news(
    article_title: str,
    article_content: str, 
) -> int:
    system_prompt = PromptsCollections.get_scoring_system_prompt()
    user_prompt = PromptsCollections.get_scoring_user_prompt()
    llm_collections = LLMCollection()

    prompt = ChatPromptTemplate.from_messages([
        ("system", system_prompt),
        ('user', user_prompt )
    ])

    # Define the output parser and prompt template
    scoring_parser = JsonOutputParser(pydantic_object=ScoringNews)
    format_instructions = scoring_parser.get_format_instructions()

    article_input = article_title + '/n' + article_content

    for llm in llm_collections.get_llms():
        try:
            llm_used = getattr(llm, 'model_name', getattr(llm, 'model', 'unknown'))
            LOGGER.info(f'LLM used: {llm_used}')
            
            scoring_chain = prompt | llm | scoring_parser 
        
            response_scoring = scoring_chain.invoke({
                'body': article_input,
                'format_instructions': format_instructions,
            })
            time.sleep(10)
            
            if not response_scoring.get('news_score'):
                LOGGER.info('Scoring response not complete, trying next llm')
                continue 

            LOGGER.info(f'[SUCCES] Scoring for url: {article_title}')
            return response_scoring.get('news_score')
            
        except json.JSONDecodeError as error: 
            LOGGER.error(f"Failed to parse JSON responsee {error}")
            continue
        
        except Exception as error:
            LOGGER.error(f"[Scoring] LLM failed with error: {error}")
            continue 

    LOGGER.error("All llm failed return None for scoring")
    return None 


if __name__ == "__main__":
    # Example usage
    article_title = "PT Adaro Energy Announces New Coking Coal Mine Acquisition"
    article_content = "PT Adaro Energy has officially announced the acquisition of a new coking coal mine in East Kalimantan, which is expected to boost its production capacity significantly."
    article_date = "2024-10-01"

    scoring_result = get_scoring_news(article_title, article_content)
    print(scoring_result)