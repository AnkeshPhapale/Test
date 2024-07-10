from ftplib import FTP
from datetime import timedelta, datetime
import shutil
import config
import os

# Create data directory if it doesn't exist
os.makedirs(config.data_dir, exist_ok=True)

# Connect to FTP server
with FTP() as ftp:
    ftp.connect(config.HOST, config.PORT)
    ftp.login(config.usr, config.pwd)
    dest = config.source_file_dir

    # Get list of files on the server
    file_list = []
    ftp.retrlines('LIST', file_list.append)

    filenames = config.filenames
    start_date = datetime.now() - timedelta(days=365)
    end_date = datetime.now()

    for file in filenames:
        if file not in ('CarIdentifiers', 'Sites'):
            while start_date < end_date:
                tname = file
                os.makedirs(os.path.join(config.data_dir, tname), exist_ok=True)
                fdate = start_date.strftime("%Y-%m-%d")
                ftp_file = f"{file}{fdate}_0000.csv"

                if ftp_file in file_list:
                    retrfile = f"RETR {ftp_file}"
                    local_file = os.path.join(config.data_dir, tname, f"{tname}{fdate}_0000.csv")
                    with open(local_file, "wb") as fp:
                        ftp.retrbinary(retrfile, fp.write)

                    # Move the file to the destination
                    f_out = os.path.join(dest, tname, f"{tname}{fdate}_0000.csv")
                    shutil.move(local_file, f_out)

                start_date += timedelta(days=1)

# Run upload script (assuming it's in the same directory)
os.system("python3 /home/oracle/ClubCar/Visage/uploadOCI.py")

# Clean up data directory
shutil.rmtree(config.data_dir)
