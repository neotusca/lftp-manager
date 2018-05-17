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
import datetime


def connect_lftp():
    #LFTP_STR="/bin/lftp -e 'mirror -N now-1day --dry-run;bye'"
    LFTP_STR="/bin/lftp -e 'mirror -N now-2day "+src_dir+" "+dst_dir+" --dry-run;bye'"
    #HOST_STR=user+':'+password+'@'+host   # ftp 
    HOST_STR=" -u "+user+", "+"sftp://"+host    # sftp ssh-key
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

    
def file_buffering(list):
    dbconn = pymongo.MongoClient("mongodb://"+mongodb_host)
    db = dbconn.MA_FILE_REPO     # db
    file_buffer = db.file_buffer # collection
    file_info = db.file_info     # collection
    "clean db"
    result = file_buffer.remove()
    print "clean :",result

    for no in range(len(list)):
        file = list.pop()
        li2 = file.split(host)    # li2 = ['   ','/dir1/dir2/file1']
        li3 = li2[-1].split('/')  # li3 = ['','dir1','dir2','file1']

        file_name = li3[-1]
        remote_dir = '/'.join(li3[:-1])

        try:
            "query db for duplicate"
            if file_info.find_one({'fullpath.remote_dir': remote_dir,'fullpath.file_name': file_name}) != None:
                #print file_info.find_one({'fullpath.remote_dir': remote_dir,'fullpath.file_name': file_name})
                print 'duplicated'
                duplicate = True
            else:
                print 'new file'
                duplicate = False
        except:
            print "query failed",sys.exc_info()[0]
            return 1

        try:
            "insert db for new file"
            diction = { "no": no+1, "script": file, "file_name": remote_dir+'/'+file_name, "duplicate":duplicate }
            #print diction
            file_buffer.insert(diction)
            print "inserted :",diction
        except:
            print "insert failed",sys.exc_info()[0]
            return 1

    dbconn.close()
    return 0

    
def register_fileinfo_db():
    dbconn = pymongo.MongoClient("mongodb://"+mongodb_host)
    db = dbconn.MA_FILE_REPO     # db
    file_buffer = db.file_buffer # collection
    file_info = db.file_info     # collection

    try:
        "query db from file_buffer"
        for file in file_buffer.find({'duplicate':False},{'_id':0,'file_name':1,'no':1}):
            list = file['file_name'].split('/')
            file_name = list[-1]
            remote_dir = '/'.join(list[:-1])
            #print remote_dir, file_name

            try:
                "insert db into file_info"
                diction = { "fullpath" : { "remote_dir" : remote_dir, "file_name" : file_name }, "status" : { "input_yn" : False, "analysis_yn" : False }, \
                            "hash" : { "md5" : "", "sha256" : "" }, "time" :{ "register_time" : datetime.datetime.utcnow(), "downloaded_time" : "" } }
                #print diction

                file_info.insert(diction)
                print "inserted :",diction
            except:
                print "insert failed",sys.exc_info()[0]
                return 1

    except:
        print "query failed", sys.exc_info()[0]
        return 1

    dbconn.close()
    return 0


def download_file():
    dbconn = pymongo.MongoClient("mongodb://"+mongodb_host)
    db = dbconn.MA_FILE_REPO     # db
    file_buffer = db.file_buffer # collection
    file_info = db.file_info     # collection
    
    try:
        "query db from file_info"
        for file in file_info.find({'status.input_yn':False,'status.analysis_yn':False},{'_id':0,'fullpath':1,'no':1}):
            #print file, type(file)
            dict =  file['fullpath']
            #print dict['remote_dir']+'/'+dict['file_name']
            str = dict['remote_dir']+'/'+dict['file_name']
            #print str, type(str)
            try:
                "query db from file_buffer"
                if file_buffer.find_one({'file_name':str,'duplicate':False},{'script':1,'_id':0}) != None:
                    #print file_buffer.find_one({'file_name':str})
                    dict = file_buffer.find_one({'file_name':str,'duplicate':False},{'script':1,'_id':0})
                    #print dict, type(dict)
                    script = dict['script']
                    print script

                    download_file_from_ftp(script)
                    update_file_info()


                else:
                    print 'not found file',str,'in buffer'
            except:
                print "query failed", sys.exc_info()[0]
                return 1

    except:
        print "query failed", sys.exc_info()[0]
        return 1

    return 0


def download_file_from_ftp(str):
    print 'ddd',str
    LFTP_STR="/bin/lftp -c '"+str+"'"
    print LFTP_STR


    #HOST_STR=user+':'+password+'@'+host   # ftp 
    #HOST_STR="-u "+user+", "+"sftp://"+host    # sftp ssh-key
    #print LFTP_STR+" "+HOST_STR
    #result = subprocess.check_output(LFTP_STR+" "+HOST_STR, shell=True)
    try:
        if subprocess.check_output(LFTP_STR, shell=True) != None:
            print "download succeed"
        #result = subprocess.check_output(LFTP_STR, shell=True)
        #print "===result:",result, type(result)
        #return result
    except:
        print 'lftp problem : already file exist or not found directory'
        return 1

    return 0





def connect_db():
    dbconn = pymongo.MongoClient("mongodb://"+mongodb_host)
    db = dbconn.MA_FILE_REPO
    files = db.file_info

    #doc = {'firstname':'taehun','lastname':'Lee'}
    #doc = { "_id" : ObjectId("5afbb077b50102515a389998"), "file_id" : NumberLong("19"), "fullpath" : { "remote_dir" : "/home/ucim/0516", "file_name" : "193" }, "status" : { "input_yn" : false, "analysis_yn" : false }, "hash" : { "md5" : MD5("00"), "sha256" : "" }, "time" : { "register_time" : Timestamp(1526445600, 0), "downloaded_time" : Timestamp(0, 0) } }
    #doc = { "_id" : ObjectId("5afbb077b50102515a389999"), "file_id" : 19, "fullpath" : { "remote_dir" : "/home/ucim/0516", "file_name" : "193" }, "status" : { "input_yn" : 0, "analysis_yn" : 0}, "hash" : { "md5" : "4047", "sha256" : "" }, "time" : { "register_time" : Timestamp(1526445600, 0), "downloaded_time" : Timestamp(0, 0) } }
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
    src_dir = '/home/ucim'
    dst_dir = '/home/dev/file_input/INPUT'
    user = 'ucim'
    password = "'ucim!!'"
    #print("FTP :",dst_dir, host, user, password)

    #mongodb_host = '192.168.200.11'  # in home
    mongodb_host = '192.168.254.223'  # in office


    LIST = connect_lftp()
    print "==1============================================"
    #print LIST
    LIST_B = get_fileinfo(LIST)
    print "==2============================================"
    #print "22",LIST_B
    #LIST_B=['get -O /home/dev/file_input/0515 sftp://ucim:@192.168.254.20/home/ucim/0515/134451','get -O /home/dev/file_input/0516 sftp://ucim:@192.168.254.20/home/ucim/0516/195']

    file_buffering(LIST_B)
    print "==3============================================"

    register_fileinfo_db()
    print "==4============================================"

    download_file()
   
    print "==5============================================"



    #################################
    # case.1
    #processing():
        # file_buffer
        # make-range and buffering
        #register_fileinfo_db(LIST_B)
        # connect_db
        # compare_range-and-db
        # register_fileinfo
        #download_file()
        #update_filefile_db()
        #############################
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

