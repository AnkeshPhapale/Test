from ftplib import FTP
from datetime import timedelta, datetime
import shutil
import config
import os
import concurrent.futures

def download_file(ftp, ftp_file, local_file):
    with open(local_file, "wb") as fp:
        ftp.retrbinary(f"RETR {ftp_file}", fp.write)

def process_file(file, start_date, end_date):
    tname = file
    os.makedirs(os.path.join(config.data_dir, tname), exist_ok=True)
    while start_date < end_date:
        fdate = start_date.strftime("%Y-%m-%d")
        ftp_file = f"{file}{fdate}_0000.csv"
        if ftp_file in flist:
            local_file = os.path.join(dest, tname, f"{tname}{fdate}_0000.csv")
            download_file(ftp, ftp_file, local_file)
        start_date += timedelta(days=1)

def main():
    os.makedirs(config.data_dir, exist_ok=True)

    ftp = FTP()
    ftp.connect(config.HOST, config.PORT)
    ftp.login(config.usr, config.pwd)

    dest = config.source_file_dir
    filenames = config.filenames

    start_date = datetime.now() - timedelta(days=365)
    end_date = datetime.now()

    # Fetch the list of files once
    file_list = []
    ftp.retrlines("LIST", file_list.append)
    flist = [f.split()[-1] for f in file_list]

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        for file in filenames:
            executor.submit(process_file, file, start_date, end_date)

    # Clean up and execute your upload script
    os.system("python3 /home/oracle/ClubCar/Visage/uploadOCI.py")
    shutil.rmtree(config.data_dir)
    ftp.quit()

if __name__ == "__main__":
    main()
