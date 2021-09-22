import sys,os,time,datetime,shutil,re,boto3,socket
from botocore.client import Config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

S3_BUCKET="mxbeta-recommendlog"
REGION="ap-south-1"
FILES_STORE=".production"
ABS_LOG_DIR="/home/ec2-user/mx-beta/log/"

def save_to_s3():
    myname = socket.getfqdn(socket.gethostname())
    myaddr = socket.gethostbyname(myname)
    myaddr=myaddr.replace(".","-")
    s3 = boto3.resource('s3', config=Config(signature_version='s3v4', region_name=REGION),aws_access_key_id="AKIAILFJJSMI4WSWTUVQ",aws_secret_access_key="bs1VudlgLHg8SEVJKPksTfGZEfo5Npr/ZIuVgM5w")
    for path in os.listdir(ABS_LOG_DIR):
        try:
            m=re.search("\d{4}-\d{2}-\d{2}",path)
            if m:
                log_time=datetime.datetime.strptime(m.group(), "%Y-%m-%d")
                if (log_time+ datetime.timedelta(days=14))<datetime.datetime.now():
                    obs_local_path=ABS_LOG_DIR+path
                    s3_path="%s/%s/%s/%s"%(FILES_STORE,log_time.year,log_time.month,path+"."+myaddr)
                    s3.meta.client.upload_file(obs_local_path, S3_BUCKET,s3_path)

        except BaseException,e:
            print e.args,e.message
def delete_log():
    for path in os.listdir(ABS_LOG_DIR):
        try:
            m=re.search("\d{4}-\d{2}-\d{2}",path)
            if m:
                log_time = datetime.datetime.strptime(m.group(), "%Y-%m-%d")
                if (log_time+ datetime.timedelta(days=14))<datetime.datetime.now():
                    abs_path = ABS_LOG_DIR+path
                    os.remove(abs_path)
                    print "delete log %s" % path

        except BaseException,e:
            print e.args,e.message,abs_path

if __name__ == "__main__":
    save_to_s3()
    delete_log()