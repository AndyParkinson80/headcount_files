# Standard Library - Core
import os
os.environ["GRPC_VERBOSITY"] = "NONE"
os.environ["GRPC_TRACE"] = ""

import sys
import json
import math
import tempfile
from pathlib import Path

# Standard Library - Time/Date
import time
from datetime import date, datetime, timedelta

# Third-party - Data Processing
import pandas as pd
import requests

# Google Cloud Platform
from google.auth import default
from google.auth.exceptions import DefaultCredentialsError
from google.oauth2 import service_account
from google.cloud import secretmanager

adp_workers_url = 'https://api.adp.com/hr/v2/workers'
cascade_workers_url = 'https://api.iris.co.uk/hr/v2/employees?%24count=true'
cascade_jobs_url = 'https://api.iris.co.uk/hr/v2/jobs?%24count=true'
cascade_hierarchy_url = 'https://api.iris.co.uk/hr/v2/hierarchy?%24count=true'

current_folder = Path(__file__).resolve().parent
data_export = True

today = date.today()
first_day_this_month = today.replace(day=1)
last_day_last_month = first_day_this_month - timedelta(days=1)
last_day_str = last_day_last_month.strftime("%Y-%m-%d")

print (last_day_str)

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

def get_secret(secret_id, version_id="latest"):
    client = secretmanager.SecretManagerServiceClient(credentials=creds)
    name = f"projects/{project_Id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

def load_keys(country):
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"    Gathering Security Information for {country} ({now_str})")
    print(f"        Loading Security Keys ({now_str})")

    # Secrets to load
    secret_ids = {
        "client_id": f"ADP-{country}-client-id",
        "client_secret": f"ADP-{country}-client-secret",
        "country_hierarchy_USA": "country_Hierarchy_USA",
        "country_hierarchy_CAN": "country_Hierarchy_CAN",
        "strings_to_exclude": "strings_to_exclude",
        "cascade_API_id": "cascade_API_id",
        "keyfile": f"{country}_cert_key",
        "certfile": f"{country}_cert_pem",
        "service_acc": "cascadeId_to_drop"
    }

    secrets = {k: get_secret(v) for k, v in secret_ids.items()}

    return (
        secrets["client_id"],
        secrets["client_secret"],
        secrets["strings_to_exclude"],
        secrets["country_hierarchy_USA"],
        secrets["country_hierarchy_CAN"],
        secrets["cascade_API_id"],
        secrets["keyfile"],
        secrets["certfile"],
        secrets["service_acc"]
    )

def load_ssl(certfile_content, keyfile_content):
    """
    Create temporary files for the certificate and keyfile contents.
    
    Args:
        certfile_content (str): The content of the certificate file.
        keyfile_content (str): The content of the key file.
    
    Returns:
        tuple: Paths to the temporary certificate and key files.
    """
    # Create temporary files for certfile and keyfile
    temp_certfile = tempfile.NamedTemporaryFile(delete=False)
    temp_keyfile = tempfile.NamedTemporaryFile(delete=False)

    try:
        # Write the contents into the temporary files
        temp_certfile.write(certfile_content.encode('utf-8'))
        temp_keyfile.write(keyfile_content.encode('utf-8'))
        temp_certfile.close()
        temp_keyfile.close()

        return temp_certfile.name, temp_keyfile.name
    
    except Exception as e:
        # Clean up in case of error
        os.unlink(temp_certfile.name)
        os.unlink(temp_keyfile.name)
        raise e

def adp_bearer(client_id,client_secret,certfile,keyfile):
    adp_token_url = 'https://accounts.adp.com/auth/oauth/v2/token'                                                                                          

    adp_token_data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret
    }
    adp_headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }
    adp_token_response = requests.post(adp_token_url, cert=(certfile, keyfile), verify=True, data=adp_token_data, headers=adp_headers)

    if adp_token_response.status_code == 200:
        access_token = adp_token_response.json()['access_token']

    return access_token

def cascade_bearer (cascade_API_id):
    cascade_token_url='https://api.iris.co.uk/oauth2/v1/token'
    
    cascade_token_data = {
        'grant_type':'client_credentials',
                    }
    cascade_headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        "Authorization": f'Basic:{cascade_API_id}'
            }

    cascade_token_response = requests.post(cascade_token_url, data=cascade_token_data, headers=cascade_headers)

    #checks the api response and extracts the bearer token
    if cascade_token_response.status_code == 200:
        cascade_token = cascade_token_response.json()['access_token']
    
    return cascade_token

def export_data(filename, variable):
    file_path = Path(current_folder) / "Data" / filename
    with open(file_path, "w", encoding='utf-8') as outfile:
        json.dump(variable, outfile, indent=4, ensure_ascii=False)

def api_count_cascade(api_response,page_size):
    response_data = api_response.json()
    total_number = response_data['@odata.count']
    api_calls = math.ceil(total_number / page_size)

    return api_calls

def api_call_cascade(cascade_token,api_url,api_params=None,api_data=None):
    cascade_api_headers = {
    'Authorization': f'Bearer {cascade_token}',
    }

    api_response = requests.get(api_url, headers = cascade_api_headers, params = api_params, json=api_data)
    time.sleep(0.6)   
   
    return api_response

def GET_workers_cascade():
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("    Retrieving current Personal Data from Cascade HR (" + time_now + ")")

    cascade_responses = []
    skip_param = 0
    page_size = 200

    api_params = {
        "$filter": (
            f"(EmploymentLeftDate eq null or EmploymentLeftDate gt {last_day_str}T00:00:00Z) "
            f"and EmploymentStartDate le {last_day_str}T00:00:00Z"
            )  
        }

    api_response = api_call_cascade(cascade_token,cascade_workers_url,api_params,None)
    api_calls = api_count_cascade(api_response,page_size)        

    for i in range(api_calls):
            skip_param = i * page_size
            
            api_params = {
                "$top": page_size,
                "$skip": skip_param,
                "$filter": (
                    f"(EmploymentLeftDate eq null or EmploymentLeftDate ge {last_day_str}T00:00:00Z) "
                    f"and EmploymentStartDate le {last_day_str}T00:00:00Z"
                )
            }

            api_response = api_call_cascade(cascade_token,cascade_workers_url,api_params)

            if api_response.status_code == 200:
                json_data = api_response.json()
                json_data = json_data['value']
                cascade_responses.extend(json_data)    

    print("         Filtering out service accounts...")
    filtered_responses = [
        record for record in cascade_responses
        if str(record.get("DisplayId")) not in service_acc
    ]

    if data_export:
        export_data("001a - Cascade Raw.json", cascade_responses)    
        export_data("001b - Cascade Filtered.json", filtered_responses)    

    return filtered_responses

def GET_jobs_cascade():
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("    Retrieving current Jobs Data from Cascade HR (" + time_now + ")")

    # Calculate six months ago
    six_months_ago = today - timedelta(days=180)
    six_months_ago_str = six_months_ago.strftime("%Y-%m-%d")

    cascade_responses = []
    skip_param = 0
    page_size = 200

    api_params = {
        "$filter": (
            f"EndDate eq null or EndDate ge {six_months_ago_str}T00:00:00Z"
            )  
        }

    api_response = api_call_cascade(cascade_token,cascade_jobs_url,api_params,None)
    api_calls = api_count_cascade(api_response,page_size)        

    for i in range(api_calls):
            skip_param = i * page_size
            
            api_params = {
                "$top": page_size,
                "$skip": skip_param,
                "$filter": (
                    f"EndDate eq null or EndDate ge {six_months_ago_str}T00:00:00Z"
                )
            }

            api_response = api_call_cascade(cascade_token,cascade_jobs_url,api_params,None)

            if api_response.status_code == 200:
                json_data = api_response.json()
                json_data = json_data['value']
                cascade_responses.extend(json_data)

    if data_export:
        export_data("001d - Cascade Jobs Raw.json", cascade_responses)

    return cascade_responses    

def GET_hierarchy_cascade():
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("    Retrieving current Hierarchy Data from Cascade HR (" + time_now + ")")

    cascade_responses = []
    skip_param = 0
    page_size = 200


    api_response = api_call_cascade(cascade_token,cascade_hierarchy_url,None,None)
    api_calls = api_count_cascade(api_response,page_size)        

    for i in range(api_calls):
            skip_param = i * page_size
            
            api_params = {
                "$top": page_size,
                "$skip": skip_param,
           }

            api_response = api_call_cascade(cascade_token,cascade_hierarchy_url,api_params,None)

            if api_response.status_code == 200:
                json_data = api_response.json()
                json_data = json_data['value']
                cascade_responses.extend(json_data)    


    if data_export:
        export_data("001c - Cascade Hierarchy Nodes.json", cascade_responses)    

    return cascade_responses

def extract_display_ids_to_csv(cascade_responses, filename="Data/display_ids.csv"):
    """
    Extract DisplayId values from cascade_responses and write to CSV file.
    
    Args:
        cascade_responses: List of dictionaries containing worker data
        filename: Output CSV filename (default: "display_ids.csv")
    """
    # Extract DisplayId values
    display_ids = [record.get('DisplayId') for record in cascade_responses]
    
    # Create a DataFrame
    df = pd.DataFrame({'DisplayId': display_ids})
    
    # Write to CSV
    df.to_csv(filename, index=False)
    
    print(f"        Exported {len(display_ids)} DisplayId values to {filename}")
    
    return df

def determine_payroll(L2, L3, L4, L6):
    # Convert to string and handle None values
    L2 = str(L2) if L2 is not None else ""
    L3 = str(L3) if L3 is not None else ""
    L4 = str(L4) if L4 is not None else "" 
    L6 = str(L6) if L6 is not None else ""
    
    # Check if L4 contains 'Lemac'
    if 'Lemac' in L4:
        payroll = "Lemac"
        return payroll
    
    if any(country in L2.lower() for country in ['germany', 'france', 'italy', 'uk']):
        if 'surveyor' in L6.lower() or 'surveyor' in L4.lower() or 'surveyor' in L3.lower():
            payroll = "Not on Payroll"
            return payroll
    
    # Check if L2 contains 'Group' or '(UK)'
    if 'Group' in L2 or '(UK)' in L2 or '(935)' in L3:
        payroll = "Acorn UK"
        return payroll
    
   
    country_mapping = {
        'Germany': 'Acorn Germany (Bureau)',
        'Italy': 'Acorn Italy (Bureau)',
        'France': 'Acorn France (Bureau)',
        'South Africa': 'Acorn South Africa (Mazars)',
        'USA': 'Acorn Inc (ADP)',
        'Australia': 'Acorn Australia (Bureau)',
        'New Zealand': 'Acorn New Zealand',
        'Isle of Man': 'Acorn Isle of Man',
        'Canada': 'Acorn Canada (ADP)',
        'Singapore': 'Acorn Singapore (Bureau)'
    }
    
    for country, payroll in country_mapping.items():
        if country in L2:
            return payroll
    
    return "Unknown Payroll"

def build_hierarchy_path(target_id):
    # Create a lookup dictionary for fast node access by Id
    node_lookup = {node['Id']: node for node in cascade_hierarchy_nodes}
    
    # Find the starting node
    if target_id not in node_lookup:
        return None, None, None, None, None, None, None
    
    current_node = node_lookup[target_id]
    hierarchy = {}
    
    # Build the hierarchy by traversing up through parents
    while current_node:
        level = current_node['Level']
        title = current_node['Title']
        hierarchy[level] = title
        
        # Move to parent node
        parent_id = current_node.get('ParentId')
        if parent_id and parent_id in node_lookup:
            current_node = node_lookup[parent_id]
        else:
            break
    
    # Return individual level values for all 6 levels
    L1 = hierarchy.get(1)
    L2 = hierarchy.get(2)
    L3 = hierarchy.get(3)
    L4 = hierarchy.get(4)
    L5 = hierarchy.get(5)
    L6 = hierarchy.get(6)

    payroll_name = determine_payroll(L2,L3,L4,L6)

    return L1, L2, L3, L4, L5, L6, payroll_name

def rearrange_cascade(cascade_responses,cascade_jobs):
    rearranged = []
    for record in cascade_responses:
        displayId = record["DisplayId"]
        knownAs = record["KnownAs"]
        surname = record["LastName"]
        nationalInsurance = record["NationalInsuranceNumber"]
        
        contServiceDateStr = record["ContinuousServiceDate"]
        contServiceDate = datetime.strptime(contServiceDateStr, '%Y-%m-%dT%H:%M:%SZ')
        contServiceDate = contServiceDate.strftime('%d/%m/%Y')
     
        contractEndDateStr = record["EmploymentLeftDate"]
        if contractEndDateStr is not None:
            contractEndDate = datetime.strptime(contractEndDateStr, '%Y-%m-%dT%H:%M:%SZ')
            contractEndDate = contractEndDate.strftime('%d/%m/%Y')
        else:
            contractEndDate = None

        for job in cascade_jobs:
            if record["Id"] == job["EmployeeId"]:
                jobTitle = job.get("JobTitle","")
                hierarchyId = job["HierarchyNodeId"]
                H1,H2,H3,H4,H5,H6,payroll_name = build_hierarchy_path(hierarchyId)

        transformed_record = {
            "Display Id": displayId,
            "Known As": knownAs,
            "Surname": surname,
            "Job Title": jobTitle,
            "Hierarchy Level 1": H1,
            "Hierarchy Level 2": H2,
            "Hierarchy Level 3": H3,
            "Hierarchy Level 4": H4,
            "Hierarchy Level 5": H5,
            "Hierarchy Level 6": H6,
            "Payroll Name": payroll_name,           
            "Cont. Service Date": contServiceDate,
            "National Insurance No.": nationalInsurance,
            "Contract End Date": contractEndDate,
        }

        rearranged.append(transformed_record)
    
    export_data("001 - Cascade rearranged.json", rearranged)    
    return rearranged

if __name__ == "__main__":
    countries = ["usa","can"]
    adp_tokens = {}
    
    creds, project_Id = google_auth()
    for c in countries:
        client_id, client_secret, strings_to_exclude, country_hierarchy_USA, country_hierarchy_CAN, cascade_API_id, keyfile, certfile, service_acc = load_keys(c)
        certfile, keyfile = load_ssl(certfile, keyfile)
        adp_tokens[c] = adp_bearer(client_id,client_secret,certfile,keyfile)
        
    adp_token_usa = adp_tokens.get('usa')
    adp_token_can = adp_tokens.get('can')
    
    cascade_token = cascade_bearer (cascade_API_id)
    service_acc = json.loads(service_acc)

    cascade_responses = GET_workers_cascade()
    cascade_jobs = GET_jobs_cascade()
    cascade_hierarchy_nodes = GET_hierarchy_cascade()
    #extract_display_ids_to_csv(cascade_responses) #Used to check parity with headcount report from cascade
    rearranged_cascade = rearrange_cascade(cascade_responses,cascade_jobs)

    df = pd.json_normalize(rearranged_cascade)

    df['Display Id'] = pd.to_numeric(df['Display Id'], errors='coerce').astype('Int64')

    df['Cont. Service Date'] = pd.to_datetime(df['Cont. Service Date'], format='%d/%m/%Y', errors='coerce')
    df['Contract End Date'] = pd.to_datetime(df['Contract End Date'], format='%d/%m/%Y', errors='coerce')

    # Export to Excel
    df.to_excel("Data/output.xlsx", index=False)
