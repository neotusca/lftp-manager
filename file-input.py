#!/usr/bin/python

"""
    1. collecting file and directory info from ftp-server
    2. register file-info into DB
    3. download file using file-info in DB
    4. update file-info into DB
"""
#import os
#import sys
import re
import subprocess


## ftp conn.  (OK)
#lftp -e 'mirror -N now-1day  --dry-run;bye' ucim:'ucim!!'@192.168.254.20 

## ftp conn.  only-script use-cache  (OK)
#lftp -e 'mirror -N now-1day  --script=$NOW  --use-cache;bye'     ucim:'ucim!!'@192.168.254.20  (NOK)
#lftp -e 'mirror -N now-1day  --script=fafa  --use-cache;bye'     ucim:'ucim!!'@192.168.254.20


def connect_lftp():
    LFTP_STR="/bin/lftp -e 'mirror -N now-1day --dry-run;bye'"
    #HOST_STR=user+':'+password+'@'+host   # ftp 
    HOST_STR="-u "+user+", "+"sftp://"+host    # sftp ssh-key
    print LFTP_STR+" "+HOST_STR
    
    result = subprocess.check_output(LFTP_STR+" "+HOST_STR, shell=True)

    return result
    

def get_fileinfo(list_string):
    print "--get_fileinfo-begin--------------"
    result=[]
    no=0
    list = list_string.split('\n')
    regex = re.compile("^get -O ")

    for file_string in list:
        Obj_A = regex.match(file_string)
        if Obj_A != None:
            print "[",no,"]",file_string
            result.append(file_string)
            no=no+1

    print "--get_fileinfo-end--------------"

    return result
    
def register_fileinfo_db(file_list):
    print "--register_fileinfo_db-begin--------------"
    print file_list

    print "--register_fileinfo_db-end--------------"

    return result


if __name__ == "__main__":
    #if len(sys.argv) < 6:
    #    sys.exit("Usage: %s LOCAL_DIR REMOTE_DIR HOST USER PASS" %
    #             (sys.argv[0],))
    #local_dir = sys.argv[1]
    #remote_dir = sys.argv[2]
    #host = sys.argv[3]
    #user = sys.argv[4]
    #password = sys.argv[5]


    host = '192.168.254.20'
    sftp_host = 'sftp://192.168.254.20'
    #src_dir = '/mnt/ftp'
    dst_dir = '/root/file_input/INPUT'
    user = 'ucim'
    password = "'ucim!!'"
    print(dst_dir, host, user, password)


    LIST = connect_lftp()
    LIST_B = get_fileinfo(LIST)
    #print type(LIST_B)
    #print LIST_B
    iterate():
        register_fileinfo_db(LIST_B)
        download_file()
        update_filefile_db()


