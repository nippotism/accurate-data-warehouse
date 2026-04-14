import os
import json

template = {
    "api_base_url": os.getenv("API_BASE_URL"),
    "access_token": os.getenv("ACCESS_TOKEN"),
    "refresh_token": os.getenv("REFRESH_TOKEN"),
}

with open("/opt/airflow/config/variables.json", "w") as f:
    json.dump(template, f)