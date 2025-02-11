import configparser as cp
import requests
from datetime import datetime
import pandas as pd
import sqlFunctions as sfn

# Parsing config file
config = cp.ConfigParser()
config.read(r'Y:\Data\Retail\WalmartMX\Development\Pikesh.Maharjan\ETL-BitBucket-API\config.ini')
access_token = config['bitbucket']['acesstoken']
base_url = config['bitbucket']['base_url']
project_key = config['bitbucket']['project_key']
ssis_server_list=config['sqlconn']['ssis_deployed_list_server']
ssis_server_database=config['sqlconn']['ssis_deployed_list_database']
ssis_server_table=config['sqlconn']['ssis_deployed_list_table']


# Convert timestamp to human-readable date
def convert_timestamp(timestamp):
    """
    Convert a timestamp (milliseconds since epoch) to a human-readable date format.
    """
    if timestamp != "N/A":
        return datetime.utcfromtimestamp(timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
    return timestamp

# Function to fetch file dates for a specific repository
def fetch_file_dates_for_repo(project_key, repo_slug, base_url, access_token):
    """
    Fetch both created and modified dates for .dtsx files in a Bitbucket repository and convert them to human-readable format.
    Returns a pandas DataFrame.
    """
    url = f"{base_url}/projects/{project_key}/repos/{repo_slug}/browse"
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        files = response.json().get("children", {}).get("values", [])
        file_details = []

        # Filter for .dtsx files and fetch commit history for created and modified dates for each file
        for file in files:
            file_path = file['path']['toString']
            if file_path.endswith('.dtsx'):
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
        
        # Convert the file details to a DataFrame
        df = pd.DataFrame(file_details)
        return df

    else:
        print(f"Failed to fetch repository details for {repo_slug}. Status Code: {response.status_code}")
        print(f"Response: {response.text}")
        return pd.DataFrame()  # Return empty DataFrame on failure

# Function to fetch all repositories in the project (with pagination)
def fetch_all_repositories(project_key, base_url, access_token):
    """
    Fetch all repositories in the given project, handling pagination.
    """
    repos = []
    url = f"{base_url}/projects/{project_key}/repos?limit=100"  # Set limit to 100 per page if needed
    headers = {"Authorization": f"Bearer {access_token}"}
    
    while url:
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            repos.extend(data.get("values", []))
            # Check if there's a next page
            url = data.get("next")
        else:
            print(f"Failed to fetch repositories. Status Code: {response.status_code}")
            print(f"Response: {response.text}")
            break
    
    return repos

# Fetch all repositories in the project
repos = fetch_all_repositories(project_key, base_url, access_token)

# Print the raw repository data to check what's being returned
print(f"Found {len(repos)} repositories.")
print("Repository slugs:")
#for repo in repos:
    #print(repo['slug'])  # Print the repository slug for debugging

# Iterate through all repositories, process those that start with 'SSIS'
all_files_details = []

for repo in repos:
    repo_slug = repo['slug']
    print(repo_slug)
    if repo_slug.startswith('ssis'):
        print(f"Processing repository: {repo_slug}")
        df_files = fetch_file_dates_for_repo(project_key, repo_slug, base_url, access_token)
        if not df_files.empty:
            df_files['repository'] = repo_slug  # Add repository name to the DataFrame
            all_files_details.append(df_files)

# Combine all file details from the repositories into one DataFrame
if all_files_details:
    final_df = pd.concat(all_files_details, ignore_index=True)
    cntrl_sql_connection=sfn.open_sql_connection(ssis_server_list,ssis_server_database)
    final_df.to_sql('__PackagesModified',cntrl_sql_connection,if_exists='replace',index=False)
else:
    print("No SSIS repositories found or no .dtsx files to process.")
