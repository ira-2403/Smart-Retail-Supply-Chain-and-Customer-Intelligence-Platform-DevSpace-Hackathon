#!/usr/bin/env python3
"""
ingestion.py — Data Preprocessing & Normalization Engine
Smart Retail Supply Chain and Customer Intelligence Platform

Reads Retail.csv and Warehouse.csv, cleans/normalizes data,
creates SKUs, performs retail↔warehouse matching, and loads
results into database.db (SQLite).
"""

import ast
import json
import os
import re
import sqlite3
import sys
from pathlib import Path

import pandas as pd

# ──────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
RETAIL_CSV = DATA_DIR / "Retail.csv"
WAREHOUSE_CSV = DATA_DIR / "Warehouse.csv"
DATABASE_PATH = BASE_DIR / "database.db"

# Column renaming maps (actual CSV names → spec names)
RETAIL_COLUMN_MAP = {
    "Transaction_ID": "order_id",
    "Date": "order_date",
    "Product": "product_list",
    "Total_Items": "quantity",
    "City": "city",
    "Store_Type": "store_type",
    "Discount_Applied": "online_flag",
}

RETAIL_KEEP_COLUMNS = list(RETAIL_COLUMN_MAP.keys())

WAREHOUSE_COLUMN_MAP = {
    "Product_Name": "product_name",
    "Stock_Quantity": "stock_quantity",
}

WAREHOUSE_KEEP_COLUMNS = list(WAREHOUSE_COLUMN_MAP.keys())

# ──────────────────────────────────────────────
# PRODUCT NORMALIZATION
# ──────────────────────────────────────────────

# Explicit plural → singular overrides for known products in the dataset
PLURAL_MAP = {
    "ONIONS": "ONION",
    "CARROTS": "CARROT",
    "POTATOES": "POTATO",
    "TOMATOES": "TOMATO",
    "EGGS": "EGG",
    "TISSUES": "TISSUE",
    "CHIPS": "CHIP",
    "PICKLES": "PICKLE",
    "SPONGES": "SPONGE",
    "MUSHROOMS": "MUSHROOM",
    "BLUEBERRIES": "BLUEBERRY",
    "STRAWBERRIES": "STRAWBERRY",
    "GRAPES": "GRAPE",
    "PEAS": "PEA",
    "ANCHOVIES": "ANCHOVY",
    "SARDINES": "SARDINE",
    "GREEN_BEANS": "GREEN_BEAN",
    "DIAPERS": "DIAPER",
    "RAZORS": "RAZOR",
    "EXTENSION_CORDS": "EXTENSION_CORD",
    "LIGHT_BULBS": "LIGHT_BULB",
    "POWER_STRIPS": "POWER_STRIP",
    "TRASH_BAGS": "TRASH_BAG",
    "TRASH_CANS": "TRASH_CAN",
    "BATH_TOWELS": "BATH_TOWEL",
    "PAPER_TOWELS": "PAPER_TOWEL",
    "CLEANING_RAGS": "CLEANING_RAG",
    "CEREAL_BARS": "CEREAL_BAR",
    "BABY_WIPES": "BABY_WIPE",
}

# Words that should NOT have trailing 's' stripped
NO_STRIP_SUFFIXES = {
    "CHEESE", "RICE", "SAUCE", "JUICE", "GREASE", "MOUSSE",
    "LETTUCE", "PRODUCE", "GLUCOSE", "PURPOSE", "CITRUS",
    "ASPARAGUS", "COUSCOUS", "HUMMUS", "OKRAS",
}


def normalize_product_name(raw_name: str) -> tuple[str, str]:
    """
    Normalize a raw product name into a canonical SKU.

    Returns:
        (normalized_sku, base_product_name)

    Examples:
        "onions"        → ("ONION", "ONION")
        "tea (jasmine)"  → ("TEA", "TEA")
        "Hair Gel"       → ("HAIR_GEL", "HAIR GEL")
        "Egg (Turkey)"   → ("EGG", "EGG")
        "Soap - Lemon"   → ("SOAP", "SOAP")
    """
    name = raw_name.strip()

    # Uppercase
    name = name.upper()

    # Remove parenthetical descriptors: "EGG (TURKEY)" → "EGG"
    name = re.sub(r"\s*\(.*?\)", "", name)

    # Remove dash-separated descriptors: "SOAP - LEMON" → "SOAP"
    name = re.sub(r"\s*-\s+.*$", "", name)

    # Remove remaining punctuation and symbols (keep letters, digits, spaces)
    name = re.sub(r"[^A-Z0-9\s]", "", name)

    # Collapse multiple spaces and strip
    name = re.sub(r"\s+", " ", name).strip()

    # Replace spaces with underscores for SKU
    sku = name.replace(" ", "_")

    # Apply explicit plural map first
    if sku in PLURAL_MAP:
        sku = PLURAL_MAP[sku]
    else:
        # Generic trailing-S rule (guarded)
        if sku not in NO_STRIP_SUFFIXES and not sku.endswith(("SS", "US", "IS")):
            if sku.endswith("S") and len(sku) > 3:
                sku = sku[:-1]

    base_product_name = sku.replace("_", " ")
    return sku, base_product_name


# ──────────────────────────────────────────────
# DATA LOADING & CLEANING
# ──────────────────────────────────────────────

def load_retail_data() -> pd.DataFrame:
    """Load, filter, clean, and explode Retail.csv."""
    print("[1/6] Loading Retail.csv ...")
    df = pd.read_csv(RETAIL_CSV)
    original_count = len(df)

    # Keep only required columns
    df = df[[c for c in RETAIL_KEEP_COLUMNS if c in df.columns]]
    df = df.rename(columns=RETAIL_COLUMN_MAP)

    # Discard rows where order_id or product_list is NULL
    null_critical = df["order_id"].isna() | df["product_list"].isna()
    discarded_critical = null_critical.sum()
    df = df[~null_critical].copy()

    # Default quantity to 1 where NULL
    df["quantity"] = df["quantity"].fillna(1).astype(int)

    # city, store_type, online_flag → keep NULL as-is (pandas NaN)

    print(f"    Loaded {original_count} rows, discarded {discarded_critical} (NULL order_id/product_list)")
    print(f"    Retained {len(df)} rows before explosion")
    return df


def explode_retail_products(df: pd.DataFrame) -> pd.DataFrame:
    """Parse product_list and explode to one row per product."""
    print("[2/6] Exploding product lists ...")

    def parse_product_list(val):
        """Safely parse a Python list literal string."""
        if pd.isna(val):
            return []
        try:
            products = ast.literal_eval(val)
            if isinstance(products, list):
                return [str(p).strip() for p in products if p]
            return [str(val).strip()]
        except (ValueError, SyntaxError):
            return [str(val).strip()]

    df["product_list"] = df["product_list"].apply(parse_product_list)
    df = df.explode("product_list", ignore_index=True)
    df = df.rename(columns={"product_list": "product_name"})

    # Drop rows where product_name became empty after explosion
    df = df[df["product_name"].astype(str).str.strip().ne("")]

    print(f"    Exploded to {len(df)} individual product rows")
    return df


def load_warehouse_data() -> pd.DataFrame:
    """Load and filter Warehouse.csv."""
    print("[3/6] Loading Warehouse.csv ...")
    df = pd.read_csv(WAREHOUSE_CSV)
    original_count = len(df)

    # Keep only required columns
    df = df[[c for c in WAREHOUSE_KEEP_COLUMNS if c in df.columns]]
    df = df.rename(columns=WAREHOUSE_COLUMN_MAP)

    # Discard rows where product_name is NULL
    null_name = df["product_name"].isna()
    discarded = null_name.sum()
    df = df[~null_name].copy()

    # stock_quantity: keep NULL as-is (never invent values)
    # Convert to nullable integer if possible
    df["stock_quantity"] = pd.to_numeric(df["stock_quantity"], errors="coerce")

    print(f"    Loaded {original_count} rows, discarded {discarded} (NULL product_name)")
    print(f"    Retained {len(df)} rows")
    return df


# ──────────────────────────────────────────────
# SKU NORMALIZATION & MATCHING
# ──────────────────────────────────────────────

def apply_normalization(retail_df: pd.DataFrame, warehouse_df: pd.DataFrame):
    """
    Apply SKU normalization to both datasets and build the products master table.

    Returns:
        (retail_df, warehouse_df, products_df)
    """
    print("[4/6] Normalizing product names and creating SKUs ...")

    # Normalize retail products
    retail_norm = retail_df["product_name"].apply(normalize_product_name)
    retail_df["normalized_sku"] = retail_norm.apply(lambda x: x[0])
    retail_df["base_product_name"] = retail_norm.apply(lambda x: x[1])

    # Normalize warehouse products
    warehouse_norm = warehouse_df["product_name"].apply(normalize_product_name)
    warehouse_df["normalized_sku"] = warehouse_norm.apply(lambda x: x[0])
    warehouse_df["base_product_name"] = warehouse_norm.apply(lambda x: x[1])

    # Build products master table from both sources
    retail_products = retail_df[["normalized_sku", "base_product_name"]].drop_duplicates()
    warehouse_products = warehouse_df[["normalized_sku", "base_product_name"]].drop_duplicates()
    products_df = pd.concat([retail_products, warehouse_products]).drop_duplicates(
        subset=["normalized_sku"], keep="first"
    ).reset_index(drop=True)

    print(f"    Retail SKUs: {retail_df['normalized_sku'].nunique()}")
    print(f"    Warehouse SKUs: {warehouse_df['normalized_sku'].nunique()}")
    print(f"    Combined product catalog: {len(products_df)} unique SKUs")

    return retail_df, warehouse_df, products_df


def match_retail_to_warehouse(retail_df: pd.DataFrame, warehouse_df: pd.DataFrame) -> pd.DataFrame:
    """
    For each retail product row, flag whether it matches a warehouse SKU.
    Never fails ingestion; unmatched products are flagged FALSE.
    """
    print("[5/6] Matching retail products to warehouse inventory ...")

    warehouse_skus = set(warehouse_df["normalized_sku"].unique())
    retail_df["warehouse_match"] = retail_df["normalized_sku"].isin(warehouse_skus).map(
        {True: "TRUE", False: "FALSE"}
    )

    matched = (retail_df["warehouse_match"] == "TRUE").sum()
    unmatched = (retail_df["warehouse_match"] == "FALSE").sum()
    print(f"    Matched: {matched} rows ({matched / len(retail_df) * 100:.1f}%)")
    print(f"    Unmatched: {unmatched} rows ({unmatched / len(retail_df) * 100:.1f}%)")

    return retail_df


# ──────────────────────────────────────────────
# SQLITE OUTPUT
# ──────────────────────────────────────────────

def write_to_database(
    retail_df: pd.DataFrame,
    warehouse_df: pd.DataFrame,
    products_df: pd.DataFrame,
):
    """Create tables and load cleaned data into database.db."""
    print("[6/6] Writing to database.db ...")

    conn = sqlite3.connect(str(DATABASE_PATH))

    # Drop existing tables if present (idempotent re-runs)
    conn.execute("DROP TABLE IF EXISTS retail_transactions")
    conn.execute("DROP TABLE IF EXISTS warehouse_inventory")
    conn.execute("DROP TABLE IF EXISTS products")

    # Create tables
    conn.execute("""
        CREATE TABLE retail_transactions (
            order_id TEXT,
            order_date TEXT,
            product_name TEXT,
            normalized_sku TEXT,
            quantity INTEGER,
            city TEXT,
            store_type TEXT,
            online_flag TEXT,
            warehouse_match TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE warehouse_inventory (
            product_name TEXT,
            normalized_sku TEXT,
            stock_quantity INTEGER
        )
    """)

    conn.execute("""
        CREATE TABLE products (
            normalized_sku TEXT PRIMARY KEY,
            base_product_name TEXT
        )
    """)

    # Prepare retail data for insertion
    retail_out = retail_df[[
        "order_id", "order_date", "product_name", "normalized_sku",
        "quantity", "city", "store_type", "online_flag", "warehouse_match"
    ]].copy()
    retail_out["order_id"] = retail_out["order_id"].astype(str)

    # Prepare warehouse data for insertion
    warehouse_out = warehouse_df[["product_name", "normalized_sku", "stock_quantity"]].copy()

    # Write to SQLite
    retail_out.to_sql("retail_transactions", conn, if_exists="append", index=False)
    warehouse_out.to_sql("warehouse_inventory", conn, if_exists="append", index=False)
    products_df.to_sql("products", conn, if_exists="append", index=False)

    # Create indexes for faster lookups
    conn.execute("CREATE INDEX IF NOT EXISTS idx_retail_sku ON retail_transactions(normalized_sku)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_warehouse_sku ON warehouse_inventory(normalized_sku)")

    conn.commit()

    # Verify counts
    for table in ["retail_transactions", "warehouse_inventory", "products"]:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"    {table}: {count} rows")

    conn.close()


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────

def main():
    print("=" * 60)
    print("INGESTION ENGINE — Smart Retail Supply Chain Platform")
    print("=" * 60)
    print()

    # Step 1: Load and clean retail data
    retail_df = load_retail_data()

    # Step 2: Explode product lists
    retail_df = explode_retail_products(retail_df)

    # Step 3: Load and clean warehouse data
    warehouse_df = load_warehouse_data()

    # Step 4: Normalize product names and create SKUs
    retail_df, warehouse_df, products_df = apply_normalization(retail_df, warehouse_df)

    # Step 5: Match retail products to warehouse inventory
    retail_df = match_retail_to_warehouse(retail_df, warehouse_df)

    # Step 6: Write to SQLite
    write_to_database(retail_df, warehouse_df, products_df)

    # ── Summary ──────────────────────────────
    print()
    print("=" * 60)

    warehouse_skus = set(warehouse_df["normalized_sku"].unique())
    retail_skus = set(retail_df["normalized_sku"].unique())
    matched_skus = retail_skus & warehouse_skus
    unmatched_skus = retail_skus - warehouse_skus

    summary = {
        "status": "SUCCESS",
        "retail": {
            "total_exploded_rows": len(retail_df),
            "unique_products": int(retail_df["product_name"].nunique()),
            "unique_skus": int(retail_df["normalized_sku"].nunique()),
        },
        "warehouse": {
            "total_rows": len(warehouse_df),
            "unique_products": int(warehouse_df["product_name"].nunique()),
            "unique_skus": int(warehouse_df["normalized_sku"].nunique()),
        },
        "matching": {
            "matched_skus": len(matched_skus),
            "unmatched_skus": len(unmatched_skus),
            "unmatched_list": sorted(unmatched_skus),
        },
        "products_catalog_size": len(products_df),
        "database": str(DATABASE_PATH),
    }

    print(json.dumps(summary, indent=2))
    print("=" * 60)
    print("Ingestion complete.")

    return summary


if __name__ == "__main__":
    main()
