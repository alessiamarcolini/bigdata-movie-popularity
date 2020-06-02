import os
import pickle
import googleapiclient.discovery
import google_auth_oauthlib.flow
from pathlib import Path

def get_authenticated_service(api_service_name, api_version, scopes, client_secret_filename, first=True):
    if first:
        credentials_filename = 'CREDENTIALS_PICKLE_FILE1'
    else:
        credentials_filename = 'CREDENTIALS_PICKLE_FILE'
    if os.path.exists(credentials_filename):
        with open(credentials_filename, 'rb') as f:
            credentials = pickle.load(f)
    else:
        client_secret_abs = Path(client_secret_filename).resolve()
        flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(client_secret_abs, scopes)
        credentials = flow.run_console()
        with open(credentials_filename, 'wb') as f:
            pickle.dump(credentials, f)
    return googleapiclient.discovery.build(
        api_service_name, api_version, credentials=credentials)