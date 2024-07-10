from ftplib import FTP
import os

# FTP server details
HOST = config.HOST
PORT = config.PORT
USERNAME = config.usr
PASSWORD = config.pwd

# Local directory to save downloaded files
LOCAL_DIR = config.data_dir

# Connect to FTP server
with FTP() as ftp:
    ftp.connect(HOST, PORT)
    ftp.login(USERNAME, PASSWORD)

    # List files matching the pattern "car%"
    file_list = []
    ftp.retrlines('NLST Charge%', file_list.append)

    # Create local directory if it doesn't exist
    os.makedirs(LOCAL_DIR, exist_ok=True)

    # Download files
    for filename in file_list:
        local_path = os.path.join(LOCAL_DIR, filename)
        with open(local_path, "wb") as local_file:
            ftp.retrbinary(f"RETR {filename}", local_file.write)
            print(f"Downloaded: {filename}")

print("All matching files downloaded successfully!")
