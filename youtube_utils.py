import os
import pickle
import googleapiclient.discovery
import google_auth_oauthlib.flow
from secrets import YOUTUBE_CLIENT_SECRET_FILENAME

def get_authenticated_service(api_service_name, api_version, scopes):
    if os.path.exists("CREDENTIALS_PICKLE_FILE"):
        with open("CREDENTIALS_PICKLE_FILE", 'rb') as f:
            credentials = pickle.load(f)
    else:
        flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(YOUTUBE_CLIENT_SECRET_FILENAME, scopes)
        credentials = flow.run_console()
        with open("CREDENTIALS_PICKLE_FILE", 'wb') as f:
            pickle.dump(credentials, f)
    return googleapiclient.discovery.build(
        api_service_name, api_version, credentials=credentials)