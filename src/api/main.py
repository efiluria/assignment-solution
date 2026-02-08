# Create a small Python API service FastAPI, read from the parquet file created in the previous step, and expose two APIs:
# API 1:
# Get order by order_id
# API 2:
# Get total sales per city
import yaml
from src.api.schemas import CitySalesResponse, OrderResponse
from fastapi import FastAPI, HTTPException
import pyarrow.parquet as pq
import pyarrow.compute as pc


def load_config(path="config/config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


app = FastAPI(title="Orders API")

config = load_config()

table = pq.read_table(config["output_file"])  # loaded once on startup


@app.get("/orders/{order_id}", response_model=OrderResponse)
def get_order(order_id: str):
    mask = pc.equal(table["order_id"], order_id)
    filtered = table.filter(mask)

    if filtered.num_rows == 0:
        raise HTTPException(status_code=404, detail="Order not found")

    order_dict = filtered.slice(0, 1).to_pylist()[0]
    print(f"Retrieved order: {order_dict}")
    return OrderResponse(**order_dict)


@app.get("/sales/by-city", response_model=list[CitySalesResponse])
def total_sales_by_city():
    df = table.to_pandas()

    df["total"] = df["price"] * df["quantity"]
    result = df.groupby("city")["total"].sum().reset_index()
    result = result.sort_values("total", ascending=False)

    return [
        CitySalesResponse(city=row["city"], total_sales=float(row["total"]))
        for _, row in result.iterrows()
    ]
