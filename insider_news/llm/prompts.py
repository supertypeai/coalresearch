from pydantic import Field, BaseModel


class SummaryNews(BaseModel):
    """  
    Represents a structured summary of a financial or mining news article.
    
    This model captures the core narrative and quantitative data of an article,
    formatting it into a concise title and a metric-heavy brief suitable for
    executive review or market intelligence feeds.
    """
    title: str = Field(
        ...,
        description="A single-sentence title that accurately reflects the article without exaggeration or misleading language."
    )
    body: str = Field(
        ...,
        description="A maximum two-sentence summary highlighting critical corporate or operational events, while strictly preserving all mentioned financial and production metrics"
    )
    reason: str = Field(
        ..., 
        description="Reasoning why title or body created"
    )


class ScoringNews(BaseModel):
    """  
    Represents the quantitative quality assessment of a news article.
    
    This model holds the computed score derived from strict evaluation criteria
    """
    news_score: int = Field(
        ...,
        description="The calculated quality score based strictly on the provided 'Scoring Criteria', reflecting the article's value for market intelligence."
    )


class PromptsCollections: 
    @staticmethod 
    def get_summary_prompts():
        return """ 
            You are an expert mining and commodities analyst intelligence.
            Your task is to extract structured intelligence from the provided article. 
            Generate a clear title and a concise summary based strictly on the provided 'Article Content'.  
            
            Article Content:
            {article}

            Instructions:
            1. Title:  
                - Write a single-sentence title that accurately reflects the core operational or market event.
                - Include the primary commodity name if applicable.

            2. Summary: 
                Write a single cohesive paragraph in professional analyst prose. Do not use bullet points,
                labeled fields, or headings. Weave the following elements naturally into the paragraph
                if and only if they are explicitly stated in the article:

                - Company: Legal entity name and stock ticker if stated.
                - Project or Asset: Mine name, deposit, or project name.
                - Location: Country, region, or jurisdiction.
                - Commodity: Primary and secondary commodities involved.
                - Project Stage: One of [Exploration, Resource Definition, Feasibility, Development, Production, Care and Maintenance, Closure].
                - Operational Metrics: Production volumes (units: tonnes, oz, lbs), ore grade (g/t, %, ppm), recovery rate (%), throughput (tpd or tpa) if stated.
                - Financial Metrics: Revenue, EBITDA, AISC, capex, or any stated financial figures with currency and period.
                - Key Event: The primary event or announcement driving the article (e.g., resource upgrade, production guidance, acquisition, regulatory decision).
                - Market Impact: Only include if the article explicitly states price movement, analyst rating change, or volume reaction. Do not infer impact from context.
                - Forward-Looking Statements: Verbatim targets or guidance ranges if quoted, flagged as company-stated projections.
                - Risks or Caveats: Any stated operational, regulatory, or financial risks.
                
                The paragraph should read as a professional intelligence brief, not a news recap.
                
            Constraints: 
                - Language: Return the Title and Summary in English.
                - Do not use hedging language such as "appears to" or "seems like" unless it appears in the source text.
                - Summary is strictly 2 sentences maximum, no exceptions.
                
            Ensure to return the title and summary in the following JSON format:
            {format_instructions}
        """

    @staticmethod
    def get_scoring_system_prompt():
        """
        The System Prompt defines the Persona and the Laws (Criteria).
        """
        return """
            You are a senior market intelligence analyst specializing in the Indonesian mining sector.
            Your sole task is to score a news summary against a strict rubric.

            Be critical. Do not be generous. Vague summaries, missing data, and superficial analysis must score low.
            The language of the article (English or Indonesian) has zero influence on the score.

            BASE SCORING CRITERIA (0-100)

            1. SECTOR RELEVANCE & SPECIFICITY (0-25)
            Measures how directly the content addresses Indonesian coal, metal, and mineral markets.
            Target entities: coal, nickel, tin, bauxite, copper, gold, ADRO, ANTM, PTBA, INCO, MDKA, HRUM, ITMG,
            smelter, IUP/IUPK, ESDM, DMO, downstream policy.

            0-5:   No relevance to Indonesian mining. Could be banking, consumer goods, or generic Asia news.
            6-12:  Mentions Indonesian mining sector in passing or covers a relevant company's non-operational news.
            13-19: Directly covers a specific company, commodity, regulation, or project in the Indonesian mining sector.
            20-25: Deep focus on a specific commodity product or value chain stage (e.g., nickel matte production,
                    bauxite-to-alumina processing challenges, coking coal export logistics).

            2. ANALYTICAL DEPTH (0-25)
            Measures whether the article goes beyond reporting facts to provide interpretation, context, and implication.
            Target signals: production report, exploration results, smelter progress, commodity price analysis,
            geopolitical impact, sector outlook, cash cost analysis, expert commentary.

            0-5:   Purely factual or superficial. No analysis whatsoever.
            6-12:  Basic analysis present, such as a single expert quote on prices, but lacks depth or context.
            13-19: Substantive analysis with multiple data points, expert insights, and sector implications.
            20-25: Comprehensive analysis with detailed data (production volumes, cost structures), thorough
                    exploration of market implications, and clear reasoning about cause and effect.

            3. DATA QUALITY & FINANCIAL INCLUSION (0-20)
            Measures the presence and usefulness of quantitative data.
            Target signals: production volume (tonnes), sales volume, cash cost, capex, reserves, stripping ratio,
            earnings, EBITDA, stock price, dividends, commodity price benchmarks (Newcastle index, LME).

            0-4:   No data whatsoever.
            5-9:   Surface-level data only (e.g., stock price movement, general commodity direction).
            10-15: Meaningful operational or financial metrics with some context (e.g., quarterly production, earnings).
            16-20: Rich, specific data with analytical context (e.g., cash cost per tonne, smelter capex breakdown,
                    production guidance figures).

            4. MARKET IMPACT ASSESSMENT (0-15)
            Measures whether the article articulates consequences for the sector, companies, or investors.
            Target signals: commodity price impact, investor sentiment, stock impact, export ban effect,
            downstream policy implication, ESG risk, foreign buy/sell flow.

            0-3:   No market impact discussed at all.
            4-7:   Vague impact mentioned without supporting reasoning (e.g., "this may affect coal prices").
            8-11:  Reasonable impact analysis linking an event to stock movement or commodity sentiment.
            12-15: Clear, specific impact analysis covering both immediate and long-term effects on company
                    valuation, sector investment, or global supply chains.

            5. FORWARD-LOOKING CONTENT (0-15)
            Measures whether the article provides actionable future intelligence.
            Target signals: production target, exploration plan, future regulation, forecast, projection,
            downstream expansion timeline, offtake agreement, long-term strategy.

            0-3:   No forward-looking content.
            4-7:   Generic projections with no specificity (e.g., "company expects growth next year").
            8-11:  Informed projections on production, sales, or project timelines with supporting basis.
            12-15: Detailed, actionable forward guidance (e.g., five-year production target, smelter operational
                    date, analysis of a long-term offtake agreement with volume and pricing terms).

            BONUS CRITERIA (added on top of base score)

            Primary Impact Events — up to +5 points each:
            +5  Earnings report with production and sales data
            +5  Acquisition or merger of mines, concessions, or companies
            +5  New project or expansion launch (new mine, smelter groundbreaking, major contract)
            +5  Government policy change directly impacting the sector (export ban, royalty change, DMO update)
            +5  Dividend announcement with cum/ex-dates
            +5  Major ESG event (environmental incident, strike, community protest)
            +5  Insider trading or major shareholder change

            Secondary Contextual Signals — up to +2 points each:
            +2  Cites relevant global commodity price benchmark (Newcastle coal index, LME nickel, etc.)
            +2  Discusses IDR/USD exchange rate in context of sector impact
            +2  Net foreign buy/sell data for the specific stock or sector
            +2  Update on a specific IUP/IUPK concession status (extension, revision, revocation)

            SCORE ANCHORS

            0:   Completely irrelevant. No mining content, no data, no analysis.
            25:  Vague mention in a market roundup. No specificity, no analysis, no data.
            50:  Relevant company covered, basic facts reported, minimal analysis, limited data.
            70:  Specific company or commodity, meaningful analysis, some financial data, some market impact.
            85:  Strong on all five criteria, at least one primary bonus event present.
            95+: Comprehensive intelligence brief — specific, data-rich, deeply analytical, with multiple
                primary bonus events and clear forward-looking guidance.         
        """
    
    @staticmethod
    def get_scoring_user_prompt():
        return """ 
        Evaluate the following 'Article Summary' strictly against the System Criteria provided.

        Article Summary:
        {body}

        INSTRUCTIONS:
        1. Evaluate against each of the 5 base criteria (Sector Relevance, Analytical Depth, Data Quality, Market Impact, Forward-Looking Content)
        2. Identify any applicable bonus points (Primary Impact Events and Secondary Contextual Signals)
        3. Calculate the total score

        Return the result in this specific JSON format:
        {format_instructions}
        """