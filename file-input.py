#!/usr/bin/python

import os
import sys
import re
import subprocess
import pymongo
from bson import ObjectId, Timestamp
import datetime


def connect_lftp():
    #LFTP_STR="/bin/lftp -e 'mirror -N now-30day "+src_dir+" "+dst_dir+" --dry-run;bye'"
    LFTP_STR="/bin/lftp -e 'mirror -N now-1day "+src_dir+" "+dst_dir+" --dry-run;bye'"
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
                #print 'duplicated'
                duplicate = True
            else:
                #print 'new-file'
                duplicate = False
        except:
            print "query failed",sys.exc_info()[0]
            return 1

        try:
            "insert db for new file"
            diction = { "no": no+1, "script": file, "file_name": remote_dir+'/'+file_name, "duplicate":duplicate }
            #print diction
            file_buffer.insert(diction)
            print "inserted :",diction['file_name'],", duplicate : ",diction['duplicate']
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
            dict =  file['fullpath']
            str = dict['remote_dir']+'/'+dict['file_name']
            print "str : ", str, type(str)
            try:
                "query db from file_buffer"
                if file_buffer.find_one({'file_name':str},{'script':1,'_id':0}) != None:  
                    dict2 = file_buffer.find_one({'file_name':str},{'script':1,'_id':0})
                    script = dict2['script']

                    if mkdir_directory(script) == 0:
                        print "mkdir succeed"
                    else:
                        print "mkdir failed"

                    if download_file_from_ftp(script) == 0:
                        #print "---",complete
                        print "---",dict['remote_dir'], dict['file_name']
                        update_file_info(dict['remote_dir'], dict['file_name'])
                    else:
                        print "can't make directory"
                        continue  # skip

                else:
                    print 'not found file',str,'in file_buffer'
            except:
                print "query failed - file_buffer - download_file()", sys.exc_info()[0]
                return 1

    except:
        print "query failed - file_info - download_file()", sys.exc_info()[0]
        return 1

    return 0

def mkdir_directory(script):
    list1 = script.split(' ')
    dir_name = list1[2]
    print "dir_name :",list1[2]

    if os.path.exists(dir_name) == False:
        os.makedirs(dir_name)
        print dir_name,"is made"
    else:
        #make dir
        print dir_name,"is already exist"

    return 0


def download_file_from_ftp(str):
    LFTP_STR="/bin/lftp -c '"+str+"'"
    print LFTP_STR

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


def update_file_info(remote_dir, file_name):
    print "AAA : ", remote_dir, file_name
    dbconn = pymongo.MongoClient("mongodb://"+mongodb_host)
    db = dbconn.MA_FILE_REPO     # db
    file_buffer = db.file_buffer # collection
    file_info = db.file_info     # collection
    #print "result : ",result, type(result)
    try:
        for file in file_info.find({'status.input_yn':False,'status.analysis_yn':False,'fullpath.remote_dir':remote_dir, 'fullpath.file_name':file_name},{'_id':0,'fullpath':1,'no':1}):
            print file
            file_info.update({'status.input_yn':False,'status.analysis_yn':False,'fullpath.remote_dir':remote_dir, 'fullpath.file_name':file_name},{'$set':{ 'status.input_yn':True, 'time.downloaded_time': datetime.datetime.utcnow()}})
            print "updated : ",file
    except:
        print "query failed at [update_file_info]", sys.exc_info()[0]
        return 1

    return 0


def connect_db():
    dbconn = pymongo.MongoClient("mongodb://"+mongodb_host)
    db = dbconn.MA_FILE_REPO
    files = db.file_info

    try:
        files.insert(doc)
    except:
        print "insert failed",sys.exc_info()[0]
    dbconn.close()
    return result



if __name__ == "__main__":


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
    print "==1:get_fileinfo============================================"
    LIST_B = get_fileinfo(LIST)
    print "==2:file_buffering=========================================="

    file_buffering(LIST_B)
    print "==3:register_fileinfo_db===================================="

    register_fileinfo_db()
    print "==4:download_file==========================================="

    download_file()
    print "==5============================================"

