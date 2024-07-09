from ftplib import FTP
import oci
from datetime import date, timedelta, datetime
import shutil
import config
import pandas as pd
import os
import logging
import oracledb
os.makedirs(config.data_dir, exist_ok=True)
ftp = FTP()
ftp.connect(config.HOST,config.PORT)
ftp.login(config.usr,config.pwd)
dest = config.source_file_dir

def getCursorADW():
    connection=oracledb.connect(
    config_dir=config.adwwallet_dir,
    user=config.adwuser,
    password=config.adwpassword,
    dsn="ccanalyticsadwnpd_low",
    wallet_location=config.adwwallet_dir,
    wallet_password=config.adwwalletpassword)
    cursor = connection.cursor()
    return cursor

def getDate():
    cursor=getCursorADW()
    print(cursor)
    cursor.execute("select TO_CHAR(FILE_DATE,'YYYY-MM-DD') AS FILE_DATE FROM VISAGE_FULL_LOAD ")
    res = cursor.fetchone()
    col_names = [row[0] for row in cursor.description]
    df = pd.DataFrame(res, columns=col_names)
    return  df['FILE_DATE'][0]
    

file_list=[]
ftp.retrlines('LIST',file_list.append)
flist=[]
for f in file_list:
    flist.append(f.split()[-1])
filenames = config.filenames

print(getDate())

for file in filenames:
    current_date=pd.to_datetime(datetime.now()) + pd.DateOffset(days=0)
    if(file=='CarIdentifiers' or file=='Sites'):
        pass       
    else:

        fdate=str(getDate())
        while(fdate<current_date):
            tname=file
            master_file=dest+ tname+"/"+ tname +"_ALL.csv"
            os.makedirs(config.data_dir+tname+"/", exist_ok=True)
            ftp_file=file+fdate+"_0000.csv"
            print(ftp_file)
            if(ftp_file in flist):
                retrfile = "RETR "+ftp_file
                with open(ftp_file, "wb") as fp:
                    ftp.retrbinary(retrfile, fp.write)
                    f_in = ftp_file
                    f_out = dest+ tname+"/"+ tname +fdate+"_0000.csv"
                    f_out2 =dest+ tname+"/"+ tname +".csv"
                    shutil.copy(f_in,f_out2)
                    shutil.move(f_in, f_out)
                    # Check if 'master_file' exists
                    if not os.path.exists(master_file):
                        # Create an empty 'master_file' if it doesn't exist
                        with open(master_file, 'w') as new_file:
                            pass  # Empty file created
                    if os.path.exists(f_out) and os.path.exists(master_file):
                         # Both files exist, proceed with reading and writing
                        with open(f_out, 'r') as f1:
                            original = f1.read()
                        with open(master_file, 'a') as f2:
                            f2.write('\n')
                            f2.write(original)
                    else:
                        print("Either 'f_out' or 'master_file' does not exist. Skipping the operation.")
            fdate=pd.to_datetime(fdate) + pd.DateOffset(days=1)

os.system("python3 /home/oracle/ClubCar/Visage/uploadOCI.py")
shutil.rmtree(config.data_dir)


ftp.quit()
