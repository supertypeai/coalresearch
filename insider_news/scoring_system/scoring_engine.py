from pydantic                       import Field, BaseModel
from langchain_core.output_parsers  import JsonOutputParser
from langchain.prompts              import PromptTemplate 
from langchain_core.runnables       import RunnableParallel

from .llms import LLMModels

import json 
from operator import itemgetter
from datetime import datetime

CRITERIA = """Revised Scoring System: Indonesian Coal, Metal & Mineral Market Intelligence
    News Article Scoring Criteria (0-100). The language used (English or Indonesian) should have **ZERO** influence on the score.

    1. Timeliness (0-10) No change from the original. This criterion is universal. Keywords: "recent", "today", "this week", "Q3 2024", "latest market movement".
        Score 0-2: Article is outdated (e.g., more than 2 weeks old).
        Score 3-5: Article is somewhat recent (published within the last 2 weeks).
        Score 6-8: Article is published within the last week.
        Score 9-10: Article is very recent (published within the last 24-48 hours).
    2. Source Credibility (0-10) Slightly modified to include industry-specific sources. Keywords: "Bloomberg", "Reuters", "Kontan", "Bisnis Indonesia", "Petromindo", "Dunia Tambang", "Ministry of Energy and Mineral Resources (ESDM)", "Minerba One Data", "IDX", "OJK".
        Score 0-2: Unknown or unreliable source.
        Score 3-5: Moderately credible source (e.g., regional news, less-known blogs).
        Score 6-8: Well-established national news outlet.
        Score 9-10: Top-tier financial news source or official government/industry publication (e.g., Reuters, ESDM press release).
    3. Clarity and Structure (0-10) No change from the original. This criterion is universal. Keywords: "clear headline", "well-structured", "organized", "informative lead".
        Score 0-2: Poorly structured and confusing.
        Score 3-5: Somewhat organized but lacks clarity.
        Score 6-8: Well-organized and easy to follow.
        Score 9-10: Excellently structured, enhancing readability and comprehension.
    4. Relevance to Indonesian Coal, Metal & Mineral Sectors (0-15) Heavily modified to focus on the target industry. Keywords: "coal", "nickel", "tin", "bauxite", "copper", "gold", "Adaro Energy (ADRO)", "Antam (ANTM)", "Bukit Asam (PTBA)", "Vale Indonesia (INCO)", "Merdeka Copper Gold (MDKA)", "Harum Energy (HRUM)", "Indo Tambangraya (ITMG)", "smelter", "mining concession (IUP)", "ESDM".
        Score 0-5: Article has no relevance to the Indonesian mining sector (e.g., focuses on banking, consumer goods).
        Score 6-10: Article mentions the mining sector in general terms or discusses a relevant company's non-operational news (e.g., general stock performance).
        Score 11-15: Article is directly about a specific Indonesian coal, metal, or mineral company, a key commodity, a new regulation, or a major project in the sector.
    5. Depth of Analysis (0-15) Modified to reflect mining industry specifics. Keywords: "production report", "exploration results", "smelter progress", "commodity price analysis", "geopolitical impact", "sector outlook", "cash cost analysis".
        Score 0-5: Superficial coverage with little to no analysis.
        Score 6-10: Includes some basic analysis, such as quoting an expert on coal prices, but lacks depth.
        Score 11-15: Offers comprehensive analysis with detailed data (e.g., production volumes, cost structures), expert insights, and thorough exploration of market implications for the Indonesian mining sector.
    6. Financial & Operational Data Inclusion (0-10) Modified to include operational metrics. Keywords: "production volume (tonnes)", "sales volume", "cash cost", "capex", "reserves", "stripping ratio", "earnings", "EBITDA", "stock price", "dividends".
        Score 0-2: No financial or operational data.
        Score 3-5: Includes basic data, such as stock price or general commodity price, with limited context.
        Score 6-8: Includes detailed financial or operational metrics (e.g., quarterly production volumes, earnings) with some analysis.
        Score 9-10: Rich in relevant data, providing extensive metrics (e.g., cash cost per tonne, capex for a new smelter) and detailed analysis specific to Indonesian mining companies.
    7. Balanced Reporting (0-5) No change from the original. This criterion is universal. Keywords: "balanced view", "multiple perspectives", "neutral tone", "pros and cons".
        Score 0-1: Highly biased or promotional.
        Score 2-3: Attempts balance but remains somewhat skewed.
        Score 4-5: Well-balanced, presenting multiple perspectives (e.g., company vs. environmental groups, different analyst opinions).
    8. Specific Commodity & Value Chain Focus (0-10) Heavily modified to reward specificity within the mining sector. Keywords: "thermal coal", "coking coal", "nickel pig iron (NPI)", "ferronickel", "bauxite", "alumina", "copper concentrate", "tin ingot", "exploration", "downstream", "smelting", "export", "Domestic Market Obligation (DMO)".
        Score 0-2: Lacks any clear commodity or value chain focus.
        Score 3-5: Discusses the "mining sector" in general terms.
        Score 6-8: Provides a focused discussion on a specific commodity (e.g., the nickel industry).
        Score 9-10: Highly specific, with in-depth coverage of a particular product (e.g., nickel matte production) or stage of the value chain (e.g., challenges in bauxite-to-alumina processing).
    9. Market & Sector Impact Relevance (0-10) Modified for sector-specific impacts. Keywords: "commodity price impact", "investor sentiment", "stock price impact", "export ban", "downstream policy", "ESG concerns".
        Score 0-2: Does not discuss or predict any market or sector impact.
        Score 3-5: Mentions potential impacts but lacks detail (e.g., "this could affect coal prices").
        Score 6-8: Discusses market impacts with reasonable detail, linking an event to potential stock price movement or commodity market sentiment.
        Score 9-10: Clearly outlines immediate and long-term impacts, with detailed analysis of how the news might influence a specific company's valuation, sector-wide investment, or global commodity supply chains.
    10. Forward-Looking Statements (0-10) Modified for mining industry specifics. Keywords: "production target", "exploration plans", "future regulations", "forecast", "projections", "downstream expansion", "long-term strategy".
        Score 0-2: No forward-looking statements.
        Score 3-5: Offers basic projections (e.g., "company expects growth next year").
        Score 6-8: Provides well-informed projections on production, sales, or project timelines.
        Score 9-10: Includes detailed and insightful projections (e.g., five-year production guidance, expected date for a smelter to be operational, analysis of a long-term offtake agreement).

    Bonus Criteria for High-Impact News (Additional Points)
        1. Primary Impact Events (Up to 5 Points Each):
        Announcement of Earnings Report with production/sales data (+5 points)
        Acquisition/Merging of mines, concessions, or companies (+5 points)
        New Project/Expansion Launch (e.g., new mine, smelter groundbreaking, major new contract) (+5 points)
        Government Policy Change directly impacting the sector (e.g., export ban, royalty change, DMO update) (+5 points)
        Dividend announcement with cum/ex-dates (+5 points)
        Major ESG Event (e.g., significant environmental incident, strike, community protest) (+5 points)
        Insider Trading or major shareholder changes (+5 points)
        2. Secondary Contextual Information (Up to 2 Points Each):
        Mentions relevant Global Commodities Prices (e.g., Newcastle coal index, LME nickel price) (+2 points)
        Discusses Rupiah (IDR) performance against the USD (+2 points)
        Data on Net Foreign Buy/Sell for the specific stock or sector (+2 points)
        Update on a specific Mining Concession (IUP/IUPK) status (extension, revision) (+2 points)

    Total Score: Example Application
        0 Score: The article is outdated, from an unknown source, and has no relevance. Example: "An Asian company made a move."
        25 Score: Vague and lacks depth. Example: "PT Merdeka Copper Gold (MDKA) was mentioned in a general IDX market summary."
        50 Score: Relevant but lacks deep analysis. Example: "PT Bukit Asam (PTBA) released its monthly coal production figures, published by a national newspaper."
        75 Score: Very recent, from a top source, highly relevant, with detailed analysis. Example: "A Reuters analysis on how Indonesia's new nickel ore export policy is expected to impact PT Antam's and PT Vale's Q4 earnings and investment in new smelters."
        80 Score: Includes the qualities of a 75-score article, plus a primary impact event. Example: "PT Adaro Energy (ADRO) officially announces the acquisition of a new coking coal mine and provides an updated production forecast for the next two years."
        90 Score: Highly detailed, with multiple primary impact events. Example: "A Bloomberg report on PT Antam's latest earnings report, which beat analyst expectations, coupled with an announcement of a new dividend policy and an update on their Haltim industrial park partnership."
        95-100 Score: A perfect-score article is a comprehensive intelligence brief in itself. Example: "Published within the last 24 hours by a top-tier source, this article details PT Freeport Indonesia's new copper smelter achieving 70% completion, provides the specific capex spent, analyzes the impact on global copper supply and local employment, includes forward-looking statements from the CEO on production targets, and references the latest LME copper prices and the IDR's performance.
    """


class ScoringNews(BaseModel):
    news_score: int = Field(description="Scoring system of a news based only on provided criteria")


def get_scoring_news(article_title: str,
                        article_content: str, 
                        article_date: str,
                        criteria: str = CRITERIA) -> ScoringNews:
    """ 
    Scoring system for news articles based on specific criteria.

    Args:
        article_title (str): Title of the article.
        article_content (str): Content of the article.
        article_date (str): Publish date of the article in 'YYYY-MM-DD' format.
        criteria (str): Scoring criteria to evaluate the article.
    
    Returns:
        ScoringNews: A Pydantic model containing the scoring result.
    """
    scoring_template = """
    You are an expert at scoring system for an industry mining article. 
    Your task is to score each article based only on 'Criteria Scoring'.

    Article Title:
    {article_title}

    Article Content:
    {article_content}

    Criteria Scoring: 
    {criteria}

    Article Publish Date:
    {article_date}

    Current Date:
    {current_date}

    Ensure the scoring is generated in the following JSON format:
    {format_instructions}
    """

    # Define the output parser and prompt template
    scoring_parser = JsonOutputParser(pydantic_object=ScoringNews)
    scoring_prompt = PromptTemplate(
        template=scoring_template, 
        input_variables=[
            "article_title",
            "article_content",
            "criteria"
            "article_date",
            "current_date"
        ],
        partial_variables={
            "format_instructions": scoring_parser.get_format_instructions()
        },
    )
    # Create a runnable scoring system that prepares the input for the LLM
    runnable_scoring_system = RunnableParallel(
        {   
            "article_title": itemgetter("article_title"),
            "article_content": itemgetter("article_content"),
            "criteria": itemgetter("criteria"),
            "article_date": itemgetter("article_date"),
            "current_date": lambda _: datetime.now()
        }
    )

    # Define the scoring chain
    scoring_chain = (
        runnable_scoring_system 
        | scoring_prompt
        | LLMModels().scoring_model
        | scoring_parser 
    )


    try: 
        # Invoke the scoring chain with the provided article details
        response_scoring = scoring_chain.invoke({
            'article_title': article_title,
            'article_content': article_content,
            'criteria': criteria,
            'article_date': article_date
        })
    
    except json.JSONDecodeError as error: 
        print(f"Failed to parse JSON responsee {error}")

    return response_scoring 


if __name__ == "__main__":
    # Example usage
    article_title = "PT Adaro Energy Announces New Coking Coal Mine Acquisition"
    article_content = "PT Adaro Energy has officially announced the acquisition of a new coking coal mine in East Kalimantan, which is expected to boost its production capacity significantly."
    article_date = "2024-10-01"

    scoring_result = get_scoring_news(article_title, article_content, article_date)
    print(scoring_result)