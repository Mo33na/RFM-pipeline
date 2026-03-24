import pandas as pd
from datetime import date, timedelta
from src.database import get_connection

def extract_data(conn):
    query = """
        SELECT customer_id, created_at, total
        FROM ecom.orders
        WHERE status != 'cancelled'
    """
    return pd.read_sql(query, conn)

def transform_rfm(orders_df):
    run_date = pd.to_datetime(date.today())
    last_90_days = run_date - timedelta(days=90)

    orders_df["created_at"] = pd.to_datetime(orders_df["created_at"]).dt.tz_localize(None)

    recency_df = (
        orders_df.groupby("customer_id", as_index=False)
        .agg(last_order_date=("created_at", "max"))
    )
    recency_df["recency_days"] = (run_date - recency_df["last_order_date"]).dt.days

    last_90_days_orders = orders_df[orders_df["created_at"] >= last_90_days]
    last_90_days_orders = last_90_days_orders.dropna(subset=["customer_id", "total"])

    frequency_df = (
        last_90_days_orders.groupby("customer_id", as_index=False)
        .agg(
            frequency_orders=("created_at", "size"),
            monetary_value=("total", "sum")
        )
    )

    rfm_df = recency_df.merge(frequency_df, on="customer_id", how="left")
    rfm_df = rfm_df.fillna({"frequency_orders": 0, "monetary_value": 0})
    rfm_df["frequency_orders"] = rfm_df["frequency_orders"].astype(int)

    rfm_df["r_rank"] = rfm_df["recency_days"].rank(method="first", ascending=True)
    rfm_df["r_score"] = pd.cut(rfm_df["r_rank"], bins=5, labels=[5,4,3,2,1]).astype(int)

    rfm_df["f_rank"] = rfm_df["frequency_orders"].rank(method="first", ascending=False)
    rfm_df["f_score"] = pd.cut(rfm_df["f_rank"], bins=5, labels=[5,4,3,2,1]).astype(int)

    rfm_df["m_rank"] = rfm_df["monetary_value"].rank(method="first", ascending=False)
    rfm_df["m_score"] = pd.cut(rfm_df["m_rank"], bins=5, labels=[5,4,3,2,1]).astype(int)

    def assign_labels(row):
        if row.r_score >= 4 and row.f_score >= 4 and row.m_score >= 4:
            return "Champions"
        elif row.f_score >= 4 and row.r_score >= 3:
            return "Loyal"
        elif row.m_score >= 4 and row.f_score <= 3:
            return "Big Spenders"
        elif row.r_score <= 2 and row.f_score >= 3:
            return "At Risk"
        elif row.r_score <= 2 and row.f_score <= 2:
            return "Hibernating"
        else:
            return "Others"

    rfm_df["rfm_segment"] = rfm_df.apply(assign_labels, axis=1)

    rfm_df["run_date"] = run_date
    rfm_df["rfm_score"] = (
        rfm_df["r_score"].astype(str) +
        rfm_df["f_score"].astype(str) +
        rfm_df["m_score"].astype(str)
    )

    final_cols = [
        "run_date", "customer_id", "recency_days",
        "frequency_orders", "monetary_value",
        "r_score", "f_score", "m_score",
        "rfm_score", "rfm_segment"
    ]

    return rfm_df[final_cols]

def load_data(conn, final_df):
    cur = conn.cursor()

    upsert_sql = """
    INSERT INTO ecom.customer_rfm_daily (
        run_date, customer_id, recency_days,
        frequency_orders, monetary_value,
        r_score, f_score, m_score,
        rfm_score, rfm_segment
    )
    VALUES (
        %(run_date)s, %(customer_id)s, %(recency_days)s,
        %(frequency_orders)s, %(monetary_value)s,
        %(r_score)s, %(f_score)s, %(m_score)s,
        %(rfm_score)s, %(rfm_segment)s
    )
    ON CONFLICT (run_date, customer_id)
    DO UPDATE SET 
        recency_days = EXCLUDED.recency_days,
        frequency_orders = EXCLUDED.frequency_orders,
        monetary_value = EXCLUDED.monetary_value,
        r_score = EXCLUDED.r_score,
        f_score = EXCLUDED.f_score,
        m_score = EXCLUDED.m_score,
        rfm_score = EXCLUDED.rfm_score,
        rfm_segment = EXCLUDED.rfm_segment,
        updated_at = CURRENT_TIMESTAMP;
    """

    records = final_df.to_dict(orient="records")

    for record in records:
        cur.execute(upsert_sql, record)

    conn.commit()
    cur.close()

def main():
    conn = get_connection()

    orders_df = extract_data(conn)
    final_df = transform_rfm(orders_df)

    load_data(conn, final_df)

    conn.close()
    print("RFM pipeline executed successfully.")

if __name__ == "__main__":
    main()
