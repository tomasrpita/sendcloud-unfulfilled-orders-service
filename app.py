import asyncio
import logging
import os
from datetime import datetime, timedelta
from time import time

import httpx
from asyncio_throttle import Throttler
from dotenv import load_dotenv
from flask import Flask

load_dotenv()

SEND_CLOUD_API_KEY = os.environ.get("SEND_CLOUD_API_KEY")
SEND_CLOUD_API_SECRET = os.environ.get("SEND_CLOUD_API_SECRET")
SEND_CLOUD_API_URL = os.environ.get("SEND_CLOUD_API_URL")

allowed_requests_per_second = 10  # Adjust as needed

# Integrations (Shops)
integrations = {
    "FRA": os.environ.get("FRA_INTEGRATION_ID"),
    "ES": os.environ.get("ES_INTEGRATION_ID"),
    "IT": os.environ.get("IT_INTEGRATION_ID"),
    "NL": os.environ.get("NL_INTEGRATION_ID"),
    # "PT": os.environ.get("PT_INTEGRATION_ID"),
}


async def fetch_shipments(country, integration_id, throttler, start_date=None):
    async with httpx.AsyncClient(
        auth=(SEND_CLOUD_API_KEY, SEND_CLOUD_API_SECRET)
    ) as client:
        endpoint = f"/integrations/{integration_id}/shipments"
        params = {"start_date": start_date} if start_date else {}
        async with throttler:
            response = await client.get(
                f"{SEND_CLOUD_API_URL}{endpoint}", params=params
            )

        if response.status_code == 200:
            logging.info(f"Getting {country} data")
            shipments = response.json()
            result = shipments["results"]

            while shipments["next"] is not None:
                async with throttler:
                    response = await client.get(shipments["next"])

                if response.status_code == 200:
                    shipments = response.json()
                    result.extend(shipments["results"])
                else:
                    logging.error(
                        f"Error getting {country} data: {response.status_code}"
                    )
                    shipments["next"] = None

            return result
        else:
            logging.error(f"Error getting {country} data: {response.status_code}")
            return None


async def get_unfulfilled_shipments(start_date):
    throttler = Throttler(
        rate_limit=allowed_requests_per_second
    )  # Adjust rate limit as needed
    tasks = [
        fetch_shipments(country, integration_id, throttler, start_date)
        for country, integration_id in integrations.items()
    ]
    results = await asyncio.gather(*tasks)
    results = [result for result in results if result is not None]

    if not results:
        logging.info("No data")
        return []

    combined_results = []
    for result in results:
        combined_results.extend(result)

    logging.info(f"Getting unfulfilled orders from {len(combined_results)} results")
    unfulfileds = [
        result
        for result in combined_results
        if result["order_status"]["id"] == "unfulfilled"
    ]
    logging.info(f"Found {len(unfulfileds)} unfulfilled orders")
    return unfulfileds


def get_aggregate_orders(unfulfileds):
    aggregated = {}

    for order in unfulfileds:
        for item in order["parcel_items"]:
            if item["sku"].startswith("DIVAIN"):
                sku = item["sku"]
                quantity = item["quantity"]
            else:
                continue
            if sku in aggregated:
                aggregated[sku] += quantity
            else:
                aggregated[sku] = quantity

    return aggregated


async def get_data():
    try:
        start = time()
        days_before = 1
        today = datetime.now()
        before = timedelta(days=days_before)

        start_date = today - before
        unfulfileds = await get_unfulfilled_shipments(start_date.strftime("%Y-%m-%d"))
        aggregated = get_aggregate_orders(unfulfileds)
        end = time()

        output = {
            "total_unfulfilled_orders": len(unfulfileds),
            "products": [
                {"sku": sku, "quantity": quantity}
                for sku, quantity in aggregated.items()
            ],
            "time_elapsed": f"{end - start} seconds",
            "start_date": start_date.strftime("%d-%m-%Y") + " 00:00:00",
            "end_date": today.strftime("%d-%m-%Y %H:%M:%S"),
        }

    except Exception as e:
        logging.error(f"Error getting data: {e}")
        output = {"error": str(e)}

    return output


# Path: app.py
app = Flask(__name__)


@app.errorhandler(500)
def handle_500(e):
    return {"error": str(e)}, 500


@app.route("/sendcloud/unfulfilled-orders/sku", methods=["GET"])
async def sendcloud_unfulfilled_orders_sku():
    # Get the data
    data = await get_data()

    # Return the data as JSON
    return data


if __name__ == "__main__":
    app.run(debug=True, port=5500)
