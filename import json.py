import json
import random
import hashlib
from datetime import datetime, date , timezone

OUTPUT_FILE = "dim_customer_400mb.json"
TOTAL_RECORDS = 1_500_000   # ~400 MB

cities = [f"City_{i}" for i in range(1, 101)]
states = [f"State_{i}" for i in range(1, 51)]
segments = ["Retail", "Corporate", "SME"]
ratings = ["A", "B", "C"]

start_ts = datetime(2024, 1, 1)

with open(OUTPUT_FILE, "w") as f:
    for i in range(1, TOTAL_RECORDS + 1):
        customer_id = f"CUST{i:07d}"

        record = {
            "CUSTOMER_ID": customer_id,
            "CUSTOMER_NAME": f"Customer_{i}",
            "EMAIL_ID": f"cust{i}@mail.com",
            "PHONE_NUMBER": f"9{random.randint(100000000,999999999)}",
            "CITY": random.choice(cities),
            "STATE": random.choice(states),
            "COUNTRY": "USA",
            "CUSTOMER_SEGMENT": random.choice(segments),
            "CUSTOMER_STATUS": "Active",
            "CREDIT_RATING": random.choice(ratings),
            "EFFECTIVE_START_DT": start_ts.isoformat(),
            "EFFECTIVE_END_DT": "9999-12-31T00:00:00",
            "IS_CURRENT": True,
            "SOURCE_SYSTEM": "CRM",
            "LOAD_DATE": date.today().isoformat(),
            "LOAD_TIMESTAMP": datetime.now(timezone.utc).isoformat()
        }

        hash_input = f"{customer_id}{record['EMAIL_ID']}"
        record["RECORD_HASH"] = hashlib.sha256(hash_input.encode()).hexdigest()

        f.write(json.dumps(record) + "\n")

print("400 MB DIM_CUSTOMER JSON generation completed.")

