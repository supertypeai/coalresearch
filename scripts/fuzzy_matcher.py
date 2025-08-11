import pandas as pd
import sqlite3
import re

from rapidfuzz import process, fuzz

def clean_company_name(name):
    """Removes common corporate prefixes/suffixes and converts to lowercase."""
    if pd.isna(name):
        return None
    return (
        re.sub(r"\b(PT|Tbk|CV|UD|PD|KSU|KUD)\b", "", str(name), flags=re.IGNORECASE)
        .lower()
        .strip()
    )

def query_company() -> pd.DataFrame:
    conn = sqlite3.connect("db.sqlite")
    company_df = pd.read_sql("SELECT id, name FROM company;", conn)
    conn.close()

    return company_df

def match_company_by_name(target_df: pd.DataFrame, target_colum: str) -> pd.DataFrame:

    company_df = query_company()

    company_df["cleaned_company_name"] = company_df["name"].apply(clean_company_name)
    cleaned_company_name_list = company_df["cleaned_company_name"].tolist()

    company_id_map = dict(zip(company_df["cleaned_company_name"], company_df["id"]))
    company_name_map = dict(zip(company_df["cleaned_company_name"], company_df["name"]))

    target_df["cleaned_company_name"] = target_df[target_colum].apply(clean_company_name)

    def match_sequence(name, scorer=fuzz.ratio, threshold=93):
        if name in company_id_map:
            return company_id_map[name], company_name_map[name]

        match = process.extractOne(name, cleaned_company_name_list, scorer=scorer)
        if match and match[1] >= threshold:
            matched_name = match[0]
            return company_id_map[matched_name], company_name_map[matched_name]
        
        return None, None

    target_df[["company_id", "company_name"]] = target_df["cleaned_company_name"].apply(
        lambda row: pd.Series(match_sequence(row))
    )

    return target_df