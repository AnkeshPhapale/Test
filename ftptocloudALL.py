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

start_date = datetime.now() - timedelta(days=365)
end_date = datetime.now()

for file in filenames:
    if(file=='CarIdentifiers' or file=='Sites'):
        pass       
    else:
        while(start_date<end_date):
            tname=file
            master_file=dest+ tname+"/"+ tname +"_ALL.csv"
            os.makedirs(config.data_dir+tname+"/", exist_ok=True)
            fdate = start_date.strftime("%Y-%m-%d")
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
                        with open(master_file, 'w') as new_file:
                            pass  # Empty file created  
                    if os.path.exists(f_out) and os.path.exists(master_file):
                        try:
                            df1 = pd.read_csv(master_file, sep="|")
                            df2 = pd.read_csv(f_out, sep="|")

                            # Merge the DataFrames (concatenating rows)
                            merged_df = pd.concat([df1, df2], ignore_index=True)

                            # Write the merged data to a new CSV file
                            merged_df.to_csv(master_file, index=False) 
                        except pd.errors.EmptyDataError:
                              shutil.copy(f_in,master_file)
                              print("One or both files are empty. Cannot merge.")

                    else:
                        print("Either 'f_out' or 'master_file' does not exist. Skipping the operation.")
         # Increment the start date by 1 day
        start_date += timedelta(days=1)


os.system("python3 /home/oracle/ClubCar/Visage/uploadOCI.py")
shutil.rmtree(config.data_dir)
ftp.quit()
