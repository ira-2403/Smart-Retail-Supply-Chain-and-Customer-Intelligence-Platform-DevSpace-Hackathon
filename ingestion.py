#!/usr/bin/env python3
from queue import Queue, Empty
import threading
import time
import ast
import re
import sqlite3
from pathlib import Path
import pandas as pd

EVENT_QUEUE = Queue()

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"

RETAIL_CSV = DATA_DIR / "Retail.csv"
WAREHOUSE_CSV = DATA_DIR / "Warehouse.csv"

DATABASE_PATH = BASE_DIR / "database.db"

# Column mapping
RETAIL_COLUMN_MAP = {
    "Transaction_ID": "order_id",
    "Date": "order_date",
    "Product": "product_list",
    "Total_Items": "quantity",
    "City": "city",
    "Store_Type": "store_type",
    "Discount_Applied": "online_flag",
}

WAREHOUSE_COLUMN_MAP = {
    "Product_Name": "product_name",
    "Stock_Quantity": "stock_quantity",
}

PLURAL_MAP = {"ONIONS": "ONION", "CARROTS": "CARROT", "EGGS": "EGG"}

NO_STRIP_SUFFIXES = {"CHEESE", "RICE", "SAUCE"}


def normalize_product_name(raw_name):
    name = raw_name.strip().upper()
    name = re.sub(r"\s*\(.*?\)", "", name)
    name = re.sub(r"\s*-\s+.*$", "", name)
    name = re.sub(r"[^A-Z0-9\s]", "", name)
    name = re.sub(r"\s+", " ", name).strip()

    sku = name.replace(" ", "_")

    if sku in PLURAL_MAP:
        sku = PLURAL_MAP[sku]
    elif sku not in NO_STRIP_SUFFIXES and sku.endswith("S"):
        sku = sku[:-1]

    return sku, sku.replace("_", " ")

def load_retail_data():
    print("[1] Loading Retail.csv")
    df = pd.read_csv(RETAIL_CSV)
    df = df.rename(columns=RETAIL_COLUMN_MAP)

    df = df.dropna(subset=["order_id", "product_list"])
    df["quantity"] = df["quantity"].fillna(1)

    return df


def explode_retail_products(df):
    print("[2] Exploding products")

    def parse(val):
        try:
            return ast.literal_eval(val)
        except:
            return [val]

    df["product_list"] = df["product_list"].apply(parse)
    df = df.explode("product_list")

    df = df.rename(columns={"product_list": "product_name"})
    return df


def load_warehouse_data():
    print("[3] Loading Warehouse.csv")
    df = pd.read_csv(WAREHOUSE_CSV)
    df = df.rename(columns=WAREHOUSE_COLUMN_MAP)
    df = df.dropna(subset=["product_name"])
    return df

def apply_normalization(retail_df, warehouse_df):

    retail_norm = retail_df["product_name"].apply(normalize_product_name)
    retail_df["normalized_sku"] = retail_norm.apply(lambda x: x[0])

    warehouse_norm = warehouse_df["product_name"].apply(normalize_product_name)
    warehouse_df["normalized_sku"] = warehouse_norm.apply(lambda x: x[0])

    products = pd.concat([
        retail_df[["normalized_sku"]],
        warehouse_df[["normalized_sku"]]
    ]).drop_duplicates()

    return retail_df, warehouse_df, products

def match_retail_to_warehouse(retail_df, warehouse_df):

    warehouse_skus = set(warehouse_df["normalized_sku"])

    retail_df["warehouse_match"] = retail_df["normalized_sku"].isin(
        warehouse_skus
    )

    return retail_df

def write_to_database(retail_df, warehouse_df, products_df):

    conn = sqlite3.connect(str(DATABASE_PATH))

    conn.execute("DROP TABLE IF EXISTS retail_transactions")
    conn.execute("DROP TABLE IF EXISTS warehouse_inventory")
    conn.execute("DROP TABLE IF EXISTS products")

    conn.execute("""
        CREATE TABLE retail_transactions(
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
        CREATE TABLE warehouse_inventory(
            product_name TEXT,
            normalized_sku TEXT,
            stock_quantity INTEGER
        )
    """)

    conn.execute("""
        CREATE TABLE products(
            normalized_sku TEXT PRIMARY KEY
        )
    """)

    # Filter to only expected columns
    warehouse_to_db = warehouse_df[["product_name", "normalized_sku", "stock_quantity"]]
    warehouse_to_db.to_sql("warehouse_inventory", conn, if_exists="append", index=False)
    products_df.to_sql("products", conn, if_exists="append", index=False)

    conn.commit()
    conn.close()

def publish_event(event_type, payload):
    EVENT_QUEUE.put({"event_type": event_type, "data": payload})


def consumer_worker():

    print("Consumer thread started")

    conn = sqlite3.connect(str(DATABASE_PATH))

    while True:
        # Blocks until an event is available
        event = EVENT_QUEUE.get()

        if event is None: # Sentinel/Poison Pill
            print("Consumer received shutdown signal")
            break

        if event["event_type"] == "retail_transaction":

            d = event["data"]

            conn.execute("""
                INSERT INTO retail_transactions(
                    order_id, order_date, product_name,
                    normalized_sku, quantity, city,
                    store_type, online_flag, warehouse_match
                )
                VALUES (?,?,?,?,?,?,?,?,?)
            """, (
                d["order_id"],
                d["order_date"],
                d["product_name"],
                d["normalized_sku"],
                d["quantity"],
                d["city"],
                d["store_type"],
                d["online_flag"],
                str(d["warehouse_match"])
            ))

    conn.commit()
    conn.close()

    print("Consumer thread finished")

def main():

    retail_df = load_retail_data()

    retail_df = explode_retail_products(retail_df)

    warehouse_df = load_warehouse_data()

    retail_df, warehouse_df, products_df = apply_normalization(
        retail_df, warehouse_df
    )

    retail_df = match_retail_to_warehouse(retail_df, warehouse_df)

    # create schema only
    write_to_database(retail_df.iloc[0:0], warehouse_df, products_df)

    # Start consumer thread
    consumer_thread = threading.Thread(target=consumer_worker, daemon=True)
    consumer_thread.start()

    print("Publishing events to queue...")

    for i, row in enumerate(retail_df.to_dict("records")):
        publish_event("retail_transaction", row)
        # Small sleep to simulate streaming data
        if i % 10 == 0:
            time.sleep(0.01)

    print("All events published. Sending shutdown signal...")
    # Send sentinel to stop consumer
    EVENT_QUEUE.put(None)

    # Wait for consumer to finish processing
    consumer_thread.join()
    print("Main execution finished")


if __name__ == "__main__":
    main()
