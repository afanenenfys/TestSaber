import csv
import logging
import sqlite3
import time

import requests

DB_PATH = "database/client_data.db"
API_BASE_URL = "http://localhost:5000"
API_KEY = "SECRET_KEY_123"
OUTPUT_FILE = "output.csv"

MAX_RETRIES = 5

# write success/fail logs to txt file
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("task.log"),
        logging.StreamHandler()
    ]
)


# query the database for customers in Manchester with orders since 2024-09-01 
# checking table showed column was city not city_name
def get_customers():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    query = """
    SELECT
        c.customer_id,
        c.name,
        c.email,
        COALESCE(SUM(o.order_total), 0) AS total_spend
    FROM customers c
    JOIN orders o
        ON c.customer_id = o.customer_id
    WHERE
        c.city = 'Manchester'
        AND o.order_date >= '2024-09-01 00:00:00'
    GROUP BY
        c.customer_id,
        c.name,
        c.email
    """

    # execute sql and gather results into a list of dictionaries
    cursor.execute(query)
    rows = cursor.fetchall()

    conn.close()

    customers = []

    for row in rows:
        customers.append(
            {
                "customer_id": row[0],
                "name": row[1],
                "email": row[2],
                "total_spend": round(float(row[3]), 2)
            }
        )

    return customers

# get social handle from enrichment API with retries and exponential backoff
# max retries set to 5 but can be defined at top of file, same for both API requests
# error response codes are logged and handled
def get_social_handle(session, email):
    delay = 1

    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(
                f"{API_BASE_URL}/enrichment",
                params={"email": email},
                timeout=10
            )

            if response.status_code == 200:
                return (
                    response.json()["social_handle"],
                    "Profile enriched"
                )

            if response.status_code == 404:
                return None, "Profile not found"

            if response.status_code in (429, 503):
                logging.warning(
                    f"Enrichment failed for {email}. "
                    f"Status={response.status_code}. Retry {attempt + 1}"
                )
                time.sleep(delay)
                delay *= 2
                continue

            return None, f"HTTP {response.status_code}"

        except requests.RequestException as ex:
            logging.warning(
                f"Enrichment exception for {email}: {ex}"
            )
            time.sleep(delay)
            delay *= 2

    return None, "Max enrichment retries exceeded"

# same as get_social_handle but for submission of data
def submit_customer(session, payload):
    delay = 1

    for attempt in range(MAX_RETRIES):
        try:
            response = session.post(
                f"{API_BASE_URL}/submission",
                json=payload,
                timeout=10
            )

            if response.status_code == 429:
                logging.warning(
                    f"Rate limited submitting "
                    f"{payload['customer_id']}"
                )
                time.sleep(delay)
                delay *= 2
                continue

            if response.status_code != 200:
                logging.warning(
                    f"Submission HTTP error "
                    f"{response.status_code}"
                )
                time.sleep(delay)
                delay *= 2
                continue

            data = response.json()

            if data["status"] == "success":
                return True, data["message"]

            logging.warning(
                f"Submission failure for "
                f"{payload['customer_id']}"
            )

            time.sleep(delay)
            delay *= 2

        except requests.RequestException as ex:
            logging.warning(
                f"Submission exception "
                f"{payload['customer_id']}: {ex}"
            )
            time.sleep(delay)
            delay *= 2

    return False, "Submission failed after retries"


def main():
    customers = get_customers()

    logging.info(
        f"Found {len(customers)} qualifying customers"
    )

    session = requests.Session()

    session.headers.update(
        {
            "X-API-KEY": API_KEY
        }
    )

    output_rows = []

    for customer in customers:
        customer_id = customer["customer_id"]

        logging.info(
            f"Processing customer {customer_id}"
        )

        social_handle, enrichment_reason = get_social_handle(
            session,
            customer["email"]
        )

        if social_handle is None:
            output_rows.append(
                {
                    **customer,
                    "social_handle": "",
                    "success": False,
                    "reason": enrichment_reason,
                }
            )
            continue

        payload = {
            **customer,
            "social_handle": social_handle,
        }

        success, submission_reason = submit_customer(
            session,
            payload
        )

        output_rows.append(
            {
                **customer,
                "social_handle": social_handle,
                "success": success,
                "reason": submission_reason,
            }
        )

        time.sleep(0.1)

    with open(
        OUTPUT_FILE,
        "w",
        newline="",
        encoding="utf-8"
    ) as csvfile:
        fieldnames = [
            "customer_id",
            "name",
            "email",
            "total_spend",
            "social_handle",
            "success",
            "reason",
        ]

        writer = csv.DictWriter(
            csvfile,
            fieldnames=fieldnames
        )

        writer.writeheader()
        writer.writerows(output_rows)

    logging.info(
        f"Output written to {OUTPUT_FILE}"
    )


if __name__ == "__main__":
    main()
