import pandas as pd
import sqlite3

def load_data(csv_path= "data/Orders.csv", db_path= "data/superstore.db"):
    orders = pd.read_csv(csv_path, encoding="latin-1")

    orders["Order Date"] = pd.to_datetime(orders["Order Date"], format="%d-%m-%Y")
    orders["Ship Date"] = pd.to_datetime(orders["Ship Date"], format="%d-%m-%Y")

    conn = sqlite3.connect(db_path)

    orders_fact = orders[[
        "Order ID","Order Date","Ship Date","Ship Mode","Customer ID","Product ID",
        "Sales","Quantity","Discount","Profit","Shipping Cost","Order Priority"
    ]].drop_duplicates()

    customers = orders[[
        "Customer ID","Customer Name","Segment","City","State",
        "Country","Postal Code","Market","Region"
    ]].drop_duplicates()

    products = orders[[
        "Product ID","Category","Sub-Category","Product Name"
    ]].drop_duplicates()

    orders_fact.to_sql("orders", conn, if_exists="replace", index=False)
    customers.to_sql("customers", conn, if_exists="replace", index=False)
    products.to_sql("products", conn, if_exists="replace", index=False)

    conn.close()
    print("Data loaded into SQLite successfully!")

if __name__ == "__main__":
    load_data()
