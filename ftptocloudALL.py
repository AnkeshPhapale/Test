from ftplib import FTP
from datetime import timedelta, datetime
import shutil
import config
import pandas as pd
import os



os.makedirs(config.data_dir, exist_ok=True)
ftp = FTP()
ftp.connect(config.HOST,config.PORT)
ftp.login(config.usr,config.pwd)
dest = config.source_file_dir 
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
                    shutil.move(f_in, f_out)
            start_date += timedelta(days=1)
os.system("python3 /home/oracle/ClubCar/Visage/uploadOCI.py")
shutil.rmtree(config.data_dir)
ftp.quit()
