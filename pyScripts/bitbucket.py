import configparser as cp
import requests
from datetime import datetime
import pandas as pd

#parsing config file
config=cp.ConfigParser()
config.read('config.ini')
access_token=config['bitbucket']['acesstoken']
base_url=config['bitbucket']['base_url']
project_key=config['bitbucket']['project_key']
repo_slug=config['bitbucket']['repo_slug']


#bicbucket data
def convert_timestamp(timestamp):
    """
    Convert a timestamp (milliseconds since epoch) to a human-readable date format.
    """
    if timestamp != "N/A":
        return datetime.utcfromtimestamp(timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
    return timestamp

def fetch_file_dates(project_key, repo_slug, base_url, access_token):
    """
    Fetch both created and modified dates for .dtsx files in a Bitbucket repository and convert them to human-readable format.
    Returns a pandas DataFrame.
    """
    # Step 1: List all files in the repository
    url = f"{base_url}/projects/{project_key}/repos/{repo_slug}/browse"
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        files = response.json().get("children", {}).get("values", [])
        file_details = []

        # Step 2: Filter for .dtsx files and fetch commit history for created and modified dates for each file
        for file in files:
            file_path = file['path']['toString']
            if file_path.endswith('.dtsx'):  # Only process .dtsx files
                commit_url = f"{base_url}/projects/{project_key}/repos/{repo_slug}/commits"
                
                # Get the most recent commit (for modified date)
                params = {"path": file_path, "limit": 1}
                commit_response = requests.get(commit_url, headers=headers, params=params)

                if commit_response.status_code == 200:
                    commit_data = commit_response.json().get("values", [])
                    if commit_data:
                        last_modified = commit_data[0]["authorTimestamp"]  # Most recent commit
                        # Get the earliest commit (for created date)
                        earliest_commit_url = f"{commit_url}?path={file_path}&limit=1&reverse=true"
                        earliest_commit_response = requests.get(earliest_commit_url, headers=headers)
                        if earliest_commit_response.status_code == 200:
                            earliest_commit_data = earliest_commit_response.json().get("values", [])
                            if earliest_commit_data:
                                created_date = earliest_commit_data[0]["authorTimestamp"]
                                file_details.append({
                                    "file": file_path,
                                    "created_date": convert_timestamp(created_date),
                                    "last_modified": convert_timestamp(last_modified)
                                })
                            else:
                                created_date = "N/A"
                                file_details.append({
                                    "file": file_path,
                                    "created_date": created_date,
                                    "last_modified": convert_timestamp(last_modified)
                                })
                else:
                    file_details.append({
                        "file": file_path,
                        "created_date": "N/A",
                        "last_modified": "N/A"
                    })
        
        # Step 3: Convert the file details to a DataFrame
        df = pd.DataFrame(file_details)
        return df

    else:
        print(f"Failed to fetch repository details. Status Code: {response.status_code}")
        print(f"Response: {response.text}")
        return pd.DataFrame()  # Return empty DataFrame on failure

# Fetch details from the repository
df_files=fetch_file_dates(project_key, repo_slug, base_url, access_token)
print(df_files)