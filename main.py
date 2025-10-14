# Standard Library - Core
import os
import io
import sys
import json
import math
import tempfile
from pathlib import Path

# Standard Library - Time/Date
import time
from datetime import datetime, timedelta,time as dt_time

# Standard Library - Data Processing
import csv
from io import StringIO
from decimal import Decimal, ROUND_HALF_UP
import shutil

# Third-party - Data Processing
import pandas as pd
import numpy as np
import requests

# Google Cloud Platform
from google.auth import default
from google.auth.exceptions import DefaultCredentialsError
from google.oauth2 import service_account
from google.cloud import secretmanager
from google.cloud import storage

adp_workers = 'https://api.adp.com/hr/v2/workers'
cascade_workers = 'https://api.iris.co.uk/hr/v2/employees?%24count=true'
cascade_workers_base = 'https://api.iris.co.uk/hr/v2/employees'

def google_auth():
    try:
        # 1. Try Application Default Credentials (Cloud Run)
        credentials, project_id = default()
        print("✅ Authenticated with ADC")
        return credentials, project_id

    except DefaultCredentialsError:
        print("⚠️ ADC not available, trying GOOGLE_CLOUD_SECRET env var...")

        # 2. Codespaces (secret stored in env var)
        secret_json = os.getenv('GOOGLE_CLOUD_SECRET')
        if secret_json:
            service_account_info = json.loads(secret_json)
            credentials = service_account.Credentials.from_service_account_info(service_account_info)
            project_id = service_account_info.get('project_id')
            print("✅ Authenticated with service account from env var")
            return credentials, project_id

        # 3. Local dev (service account file path)
        file_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if file_path and os.path.exists(file_path):
            credentials = service_account.Credentials.from_service_account_file(file_path)
            with open(file_path) as f:
                project_id = json.load(f).get("project_id")
            print("✅ Authenticated with service account from file")
            return credentials, project_id

        raise Exception("❌ No valid authentication method found")

creds,project_id = google_auth

print (creds)
print (project_id)