#!/usr/bin/python

"""
    1. collecting file and directory info from ftp-server
    2. register file-info into DB
    3. download file using file-info in DB
    4. update file-info into DB
"""
import sys
import re
import subprocess
import pymongo
from bson import ObjectId, Timestamp


def connect_lftp():
    LFTP_STR="/bin/lftp -e 'mirror -N now-1day --dry-run;bye'"
    #HOST_STR=user+':'+password+'@'+host   # ftp 
    HOST_STR="-u "+user+", "+"sftp://"+host    # sftp ssh-key
    print LFTP_STR+" "+HOST_STR
    result = subprocess.check_output(LFTP_STR+" "+HOST_STR, shell=True)
    return result
    

def get_fileinfo(list_string):
    result=[]
    no=0
    list = list_string.split('\n')
    regex = re.compile("^get -O ")
    for file_string in list:
        Obj_A = regex.match(file_string)
        if Obj_A != None:
            #print "[",no,"]",file_string
            result.append(file_string)
            no=no+1
    return result

def processing_tmp(list):
    range_size=10
    list_tmp=[]
    print "processing:",len(list)

    #range_index = range(0, len(list), range_size)

    i=0
    for file in range(len(list)):
         if i!=10: 
             list_tmp.append(list.pop())
             i=i+1
         else:
             print "==",i,"=============="
             for afile in list_tmp:
                 print i, afile
             #print i, len(list_tmp), list_tmp
             #print list
             print list_tmp
             i=0
             list_tmp=[]
    
def file_buffering(list):
    
    dbconn = pymongo.MongoClient("mongodb://"+mongodb_host)
    db = dbconn.MA_FILE_REPO
    files = db.file_buffer

    for no in range(len(list)):
        diction = { "no": no, "filename": "'"+list.pop()+"'" }
        #print type(diction), diction

        try:
            files.insert(diction)
            print "inserted :",diction
        except:
            print "insert failed",sys.exc_info()[0]

    dbconn.close()
    return 0

    
def register_fileinfo_db(file_list):
    print "--register_fileinfo_db-begin--------------"
    print file_list
    """
    [ 6 ] get -O /home/dev/file_input/0515 sftp://ucim:@192.168.254.20/home/ucim/0515/134451
    [ 9 ] get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/195
    """
    """
    1. connect_db
    2. compare_filename-list_and_db
    3. insert_data

    """
    print "--register_fileinfo_db-end--------------"

    #return result


def connect_db():
    
    dbconn = pymongo.MongoClient("mongodb://"+mongodb_host)
    db = dbconn.MA_FILE_REPO
    files = db.file_info

    #doc = {'firstname':'taehun','lastname':'Lee'}
    #doc = { "_id" : ObjectId("5afbb077b50102515a389998"), "file_id" : NumberLong("19"), "fullpath" : { "remote_dir" : "/home/ucim/0516", "file_name" : "193" }, "status" : { "input_yn" : false, "analysis_yn" : false }, "hash" : { "md5" : MD5("00"), "sha256" : "" }, "time" : { "register_time" : Timestamp(1526445600, 0), "downloaded_time" : Timestamp(0, 0) } }
    doc = { "_id" : ObjectId("5afbb077b50102515a389999"), "file_id" : 19, "fullpath" : { "remote_dir" : "/home/ucim/0516", "file_name" : "193" }, "status" : { "input_yn" : 0, "analysis_yn" : 0}, "hash" : { "md5" : "4047", "sha256" : "" }, "time" : { "register_time" : Timestamp(1526445600, 0), "downloaded_time" : Timestamp(0, 0) } }

    #print type(doc), doc <-- dictionary
 

    try:
        files.insert(doc)
    except:
        print "insert failed",sys.exc_info()[0]

    dbconn.close()

    #return result





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
    #print("FTP :",dst_dir, host, user, password)

    mongodb_host = '192.168.200.11'


    #LIST = connect_lftp()
    #LIST_B = get_fileinfo(LIST)
    #LIST_B=['get -O /home/dev/file_input/0515 sftp://ucim:@192.168.254.20/home/ucim/0515/134451','get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/195']
    LIST_B=['get -O /home/dev/file_input sftp://ucim:@192.168.254.20/home/ucim/touch-at-0516-0921', 'get -O /home/dev/file_input/.cache/abrt sftp://ucim:@192.168.254.20/home/ucim/.cache/abrt/lastnotification', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/19', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/195', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1950', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1951', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1952', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1953', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1954', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1955', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1956', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1957', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1958', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1959', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/196', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1960', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1961', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1962', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1963', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1964', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1965', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1966', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1967', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1968', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1969', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/197', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1970', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1971', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1972', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1973', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1974', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1975', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1976', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1977', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1978', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1979', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/198', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1980', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1981', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1982', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1983', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1984', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1985', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1986', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1987', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1988', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1989', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/199', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1990', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1991', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1992', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1993', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1994', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1995', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1996', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1997', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1998', 'get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/1999']
    #print type(LIST_B), LIST_B
    print len(LIST_B)

    #register_fileinfo_db(LIST_B)
    #connect_db()

    file_buffering(LIST_B)

    # case.1
    #processing():
        # make-range and buffering
        #register_fileinfo_db(LIST_B)
        # connect_db
        # compare_range-and-db
        # register_fileinfo
        #download_file()
        #update_filefile_db()
 
    #################################
    # case.2
    # get_fileinfo_from_ftp
    #     connect_lftp()  
    #     get_fileinfo()  
    # register_fileinfo_to_db
    #     connect_db()  
    #     compare_range_and_db
    #     register_fileinfo
    # download_file_from_ftp
    #     download_file
    # update_fileinfo_to_db
    #     update_fileinfo
    ##################################

