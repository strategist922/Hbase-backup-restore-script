import time
import datetime
from datetime import date
import sys
import os
from subprocess import call

tablename=sys.argv[1]
backupDst=sys.argv[2]
isfull="nofull"
if len(sys.argv)==4:
        isfull=sys.argv[3]
today=date.today()
monthStr=today.strftime("%Y%m")
dayStr=today.strftime("%Y%m%d")
monthPath=backupDst+os.sep+monthStr
print monthPath

def createFolderInHadoop(path):
        try:
                retcode = call("hadoop dfs -test -e "+path, shell=True)
                if retcode != 0:
                        os.system("hadoop dfs -mkdir "+path)
                else:
                        print path+" is already existed"
        except OSError, e:
                print >>sys.stderr, "Execution failed:", e

createFolderInHadoop(monthPath)



if today.day == 1 or isfull == "full":
        backupSubFolder= monthPath+os.sep+dayStr+".full"+os.sep+tablename
        #createFolderInHadoop(backupSubFolder)
        cmd="hbase org.apache.hadoop.hbase.mapreduce.Export "+tablename+" "+ backupSubFolder
else:

        yesterday=datetime.date.today()- datetime.timedelta(days=1)
        todayTimeStamp=time.mktime(today.timetuple())
        yesTimeStamp=time.mktime(yesterday.timetuple())
        backupSubFolder=monthPath+os.sep+dayStr+os.sep+tablename
        #createFolderInHadoop(backupSubFolder)
        cmd="hbase org.apache.hadoop.hbase.mapreduce.Export %s %s %s %s"%(tablename, backupSubFolder,"1",str(int(todayTimeStamp)*1000))

print cmd

os.system(cmd)

