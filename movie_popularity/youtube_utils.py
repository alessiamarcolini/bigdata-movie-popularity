import os
import pickle
from pathlib import Path
from secrets import YOUTUBE_CLIENT_SECRET_FILENAMES

import google_auth_oauthlib.flow
import googleapiclient.discovery


def get_authenticated_service(api_service_name, api_version, scopes, n_tries=0):
    if n_tries == 0:
        credentials_filename = "CREDENTIALS_PICKLE_FILE1"
    elif n_tries == 1:
        credentials_filename = "CREDENTIALS_PICKLE_FILE"
    elif n_tries == 2:
        credentials_filename = "CREDENTIALS_PICKLE_FILE2"
    elif n_tries == 3:
        credentials_filename = "CREDENTIALS_PICKLE_FILE3"
    elif n_tries == 4:
        credentials_filename = "CREDENTIALS_PICKLE_FILE5"
    elif n_tries == 5:
        credentials_filename = "CREDENTIALS_PICKLE_FILE6"
    elif n_tries == 6:
        credentials_filename = "CREDENTIALS_PICKLE_FILE7"
    elif n_tries == 7:
        credentials_filename = "CREDENTIALS_PICKLE_FILE8"
    elif n_tries == 8:
        credentials_filename = "CREDENTIALS_PICKLE_FILE9"
    elif n_tries == 9:
        credentials_filename = "CREDENTIALS_PICKLE_FILE10"
    elif n_tries == 10:
        credentials_filename = "CREDENTIALS_PICKLE_FILE11"
    elif n_tries == 11:
        credentials_filename = "CREDENTIALS_PICKLE_FILE12"
    elif n_tries == 12:
        credentials_filename = "CREDENTIALS_PICKLE_FILE13"
    elif n_tries == 13:
        credentials_filename = "CREDENTIALS_PICKLE_FILE14"
    elif n_tries == 14:
        credentials_filename = "CREDENTIALS_PICKLE_FILE15"
    elif n_tries == 15:
        credentials_filename = "CREDENTIALS_PICKLE_FILE16"
    elif n_tries == 16:
        credentials_filename = "CREDENTIALS_PICKLE_FILE17"
    elif n_tries == 17:
        credentials_filename = "CREDENTIALS_PICKLE_FILE18"
    elif n_tries == 18:
        credentials_filename = "CREDENTIALS_PICKLE_FILE19"
    elif n_tries == 19:
        credentials_filename = "CREDENTIALS_PICKLE_FILE20"

    else:
        print("boh")
    client_secret_filename = YOUTUBE_CLIENT_SECRET_FILENAMES[n_tries]

    if os.path.exists(credentials_filename):
        with open(credentials_filename, "rb") as f:
            credentials = pickle.load(f)
    else:
        client_secret_abs = Path(client_secret_filename).resolve()
        flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
            client_secret_abs, scopes
        )
        credentials = flow.run_console()
        with open(credentials_filename, "wb") as f:
            pickle.dump(credentials, f)

    return googleapiclient.discovery.build(
        api_service_name, api_version, credentials=credentials
    )
