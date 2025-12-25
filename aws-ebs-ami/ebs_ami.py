import boto3
import datetime
import pandas as pd
import csv
s3=boto3.client('s3')
ssm=boto3.client('ssm')
sts=boto3.client('sts')
sns=boto3.client('sns')
li=['AccountId','Resourse','Service','Region']
snap=[]
ami=[]
now_time=datetime.datetime.now().date()
diff_time=0

def lambda_handler(event,context):
    rolearnlist_from_ssm=ssm.get_parameter(Name='old-snaps-iam')
    rolearnlist=rolearnlist_from_ssm['Parameter']['Value'].split(",")
    #rolearnlist=['arn:aws:iam::494829558485:role/old_snap_role']
    for x in rolearnlist:
        get_old_snap(x)
    excel_writer()
    #create_s3()
    #notification()

def taglist(AccountId,resourse,item,Service,Region,Age,Description):
    d={'AccountId':AccountId,'Resourse':resourse,'Service':Service,'Region':Region,'Description':Description,'Snapshot Age':Age}
    try:
        for i in item:
            d[i['Key']]=i['Value']
            Keys=(list(d.keys()))
            for i in Keys:
                if i not in li:
                    li.append(i)
    except:
        d={**d,**item}
        Keys=(list(item.keys()))
        for i in Keys:
            if i not in li:
                li.append(i)
    #print(resourse,"  ",d)
    snap.append(d)

def taglist_ami(snapname,AccountId,resourse,item,Service,Region,Age,Description):
    d={'AccountId':AccountId,'Resourse Name':snapname,'Resourse':resourse,'Service':Service,'Region':Region,'Description':Description,'AMI Age':Age}
    try:
        for i in item:
            d[i['Key']]=i['Value']
            Keys=(list(d.keys()))
            for i in Keys:
                if i not in li:
                    li.append(i)
    except:
        d={**d,**item}
        Keys=(list(item.keys()))
        for i in Keys:
            if i not in li:
                li.append(i)
    #print(resourse,"  ",d)
    ami.append(d)

def get_old_snap(rolearn):
    #ec2 = boto3.client('ec2', region_name=reg)
    AccountId=[rolearn.split(':')[4]]
    awsaccount = sts.assume_role(RoleArn=rolearn,RoleSessionName='awsaccount_session')
    ACCESS_KEY = awsaccount['Credentials']['AccessKeyId']
    SECRET_KEY = awsaccount['Credentials']['SecretAccessKey']
    SESSION_TOKEN = awsaccount['Credentials']['SessionToken']
    for reg in ['ap-south-1','us-east-1','ap-northeast-2']:
        ec2 = boto3.client("ec2",aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY, aws_session_token=SESSION_TOKEN, region_name=reg )
        paginator = ec2.get_paginator('describe_snapshots')
        for page in paginator.paginate(OwnerIds=AccountId):
            #print(page['Snapshots'])
            for j in range(len(page['Snapshots'])):
                snapstart=page['Snapshots'][j]['StartTime'].date()
                snapage=str((now_time-snapstart).days)
                if int(snapage)>=0:
                    snapid=str(page['Snapshots'][j]['SnapshotId'])
                    try:
                        snapdesc=str(page['Snapshots'][j]['Description'])
                    except:
                        snapdesc=''
                    try:
                        snaptags=page['Snapshots'][j]['Tags']
                    except:
                        snaptags={}

                    print(snapid,"  ",snapdesc,"  ",snapage,"  ",snaptags)
                    taglist(AccountId[0],snapid,snaptags,'EBS Snapshots',reg,snapage,snapdesc)

        '''paginator = ec2.describe_images(Owners=AccountId)
        for page in paginator['Images']:
            #print(page)
            snapstart=str(page['CreationDate'][0:10])
            snapstart = datetime.datetime.strptime(snapstart, '%Y-%m-%d').date()
            snapage=str((now_time-snapstart).days)
            if int(snapage)>=0:
                snapid=str(page['ImageId'])
                try:
                    snapdesc=str(page['Description'])
                except:
                    snapdesc=''
                try:
                    snaptags=page['Tags']
                except:
                    snaptags={}
                try:
                        snapname=page['Name']
                except:
                    snapname={}
                #print(snapname,snapid,"  ",snapdesc,"  ",snapage,"  ",snaptags)
                taglist_ami(snapname,AccountId[0],snapid,snaptags,'Machine Image',reg,snapage,snapdesc)'''

def excel_writer():
    df=pd.DataFrame(snap)
    df1=df.fillna('Not Tagged')
    dfami=pd.DataFrame(ami)
    df2=dfami.fillna('Not Tagged')
    with pd.ExcelWriter('ami.xlsx') as writer:
        df1.to_excel(writer,sheet_name='EBS Snapshot',index=False)
        df2.to_excel(writer,sheet_name='AMI',index=False)
                
def create_s3():
    s3.upload_file('get_ebs_snaps.csv',"tag-csv",'get_ebs_snap.csv')

def notification():
    sns=boto3.client('sns',region_name='ap-south-1')
    sns.publish(TopicArn="arn:aws:sns:ap-south-1:12345678942:send_certificate_file",
    Subject="EBS Snapshots and AMI's older than 3 months",
    Message="EBS Snapshots and AMI's older than 3 months.\n\nPlease check S3 bucket :- tags-csv\nCSV File Name :- get_ebs_snap.csv")


lambda_handler(str, str)