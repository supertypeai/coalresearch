from insider_news.preprocessing.utils.helper import run_extract_commodities, manual_scoring_time, dedup_comodities
from insider_news.preprocessing.scoring_engine import get_scoring_news 
from insider_news.preprocessing.summary_engine import get_summary 
from insider_news.scrapers.article_content import get_article_body

import logging 
import time

LOGGER = logging.getLogger(__name__)


def build_enriched_article(
    payload: dict[str, any]            
):
    final_data = {}

    source = payload.get("source").strip() 
    timestamp = payload.get("timestamp")

    try:
        article_content = get_article_body(source)

        title, body = get_summary(article_content)
        if title and body:
            LOGGER.info(f'[SUCCES] Summarize for url {source}')
        
        comodities = run_extract_commodities(title, body, article_content)
        final_comodities = dedup_comodities(comodities)

        score = get_scoring_news(title, body)
        manual_score = manual_scoring_time(timestamp)

        final_score = score + manual_score

        final_data.update({
            'source': source,
            'timestamp': timestamp,
            'title': title, 
            'body': body, 
            'commodity_type': final_comodities, 
            'score': final_score
        })

        time.sleep(2)
        return final_data

    except Exception as error:
        LOGGER.error(
            f"[ERROR] A critical, unexpected error occurred in generate_article_async for {source}: {error}",
            exc_info=True
        )
        return None  



