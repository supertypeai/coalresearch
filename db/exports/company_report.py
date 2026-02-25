import sqlite3
import json
import os

def export_top_10_coal_companies():
    """
    Exports the top 10 coal mining companies by 2024 production volume to a JSON file.
    Includes production volume, total revenue, and total assets.
    """
    db_file = os.path.join(os.path.dirname(__file__), '..', '..', 'db.sqlite')
    # Resolve the absolute path
    db_file = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'db.sqlite'))
    
    conn = None
    try:
        # Check if file exists
        if not os.path.exists(db_file):
            print(f"Error: Database file not found at {db_file}")
            # Try current directory as fallback
            db_file = os.path.abspath('db.sqlite')
            if not os.path.exists(db_file):
                 return

        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        print(f"Connected to {db_file}. Querying top 10 coal companies for 2024...")

        query = """
        SELECT 
            c.name,
            c.idx_ticker,
            json_extract(cp.commodity_stats, '$.production_volume') as production_volume,
            json_extract(cp.commodity_stats, '$.unit') as unit,
            cf.revenue as total_revenue,
            cf.assets as total_assets
        FROM 
            company c
        JOIN 
            company_performance cp ON c.id = cp.company_id
        LEFT JOIN 
            company_financials cf ON c.id = cf.company_id AND cf.year = 2024
        WHERE 
            cp.year = 2024 
            AND cp.commodity_type = 'Coal'
        ORDER BY 
            production_volume DESC
        LIMIT 12;
        """

        cursor.execute(query)
        rows = cursor.fetchall()
        
        # Get column names
        columns = [description[0] for description in cursor.description]
        
        results = []
        for row in rows:
            results.append(dict(zip(columns, row)))

        # Add a note about market cap
        for res in results:
            res['market_cap'] = "N/A" # Not available in database

        output_file = os.path.join(os.path.dirname(__file__), 'top_10_coal_companies_2024.json')
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=4)

        print(f"Exported top 10 coal companies to {output_file}")
        
        # Also print to stdout for the user
        print(json.dumps(results, indent=4))

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    export_top_10_coal_companies()
