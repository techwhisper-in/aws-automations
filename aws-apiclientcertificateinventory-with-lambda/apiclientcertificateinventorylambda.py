import boto3
from datetime import datetime,date, timedelta
import calendar
import csv
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

ses_client = boto3.client('ses')
s3=boto3.client('s3')
ssm=boto3.client('ssm')
sts=boto3.client('sts')


def lambda_handler(event,context):
    file_name="certificate_inventory_test_"+str(date.today().strftime("%d-%m-%y"))+'_'+str((datetime.now().strftime("%H:%M:%S")))+'.csv'
    with open("/tmp/"+file_name,'w',encoding='UTF-8',newline='') as f:
            header=[['AccountId','AccountName','Clientid','Region','Description','Creation_Date','Expiration_Date','Notification_Date']]
            writer=csv.writer(f)
            writer.writerows(header)
    list_cross_accounts(file_name,all_dates_month())
    create_s3(file_name)
    send_email_with_attachment(file_name)
    return 'success'
    

def list_cross_accounts(st,func):
    file_name=st
    li=list(func)
    region_name=['ap-south-1','us-east-1']
    rolearnlist_from_ssm=ssm.get_parameter(Name='rolearnlist')
    rolearnlist=rolearnlist_from_ssm['Parameter']['Value'].split(",")
    for x in rolearnlist:
        for reg in region_name:
            data_cross_acc(x,reg,li,file_name)
            

def data_cross_acc(x,reg,li,st):
    file_name=st
    exp_date=li
    AWS_REG=reg
    rolearn=x
    x=x.split(":")
    if x[4]=="494829558485":
        acc_id=x[4]
        acc_name=('shared_account')
    elif x[4]=="290126196274":
        acc_id=x[4]
        acc_name=('cross_account')
    elif x[4]=="075536595857":
        acc_id=x[4]
        acc_name=('cross_account2')
    awsaccount = sts.assume_role(
        RoleArn=rolearn,
        RoleSessionName='awsaccount_session'
    )
    ACCESS_KEY = awsaccount['Credentials']['AccessKeyId']
    SECRET_KEY = awsaccount['Credentials']['SecretAccessKey']
    SESSION_TOKEN = awsaccount['Credentials']['SessionToken']
    stsclient = boto3.client("apigateway",aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY, aws_session_token=SESSION_TOKEN, region_name=AWS_REG )
    response = stsclient.get_client_certificates()
    with open("/tmp/"+file_name,'a+',encoding='UTF-8',newline='') as f:
        writer=csv.writer(f)
        for i in range(len(response['items'])):
            if str((response['items'][i]['expirationDate']).date()) in exp_date:
                try:
                    description=response['items'][i]['description']
                except:
                    description=''
                writer.writerows([[acc_id,acc_name,response['items'][i]['clientCertificateId'],AWS_REG,description,str(response['items'][i]["createdDate"]),str(response['items'][i]['expirationDate']),str(((response['items'][i]['expirationDate']).date()-datetime.now().date()).days)]])


def all_dates_month():
    d=[]
    for i in range(11,13):
        todays_date = date.today()
        start_date = date(todays_date.year,todays_date.month,todays_date.day)
        days_in_month = calendar.monthrange(start_date.year, start_date.month)[1]
        for j in range(i):
            next_mont=(start_date + timedelta(days=days_in_month))
            start_date=date(next_mont.year,next_mont.month,next_mont.day)
            days_in_month = calendar.monthrange(start_date.year, start_date.month)[1]
        first_date = date(start_date.year, start_date.month, 1)
        last_date = date(start_date.year, start_date.month,days_in_month)
        delta = last_date - first_date
        d1= [(first_date + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(delta.days + 1)]
        d=d+d1
    print(d)
    return d

    
def create_s3(file_name):
    file_name=file_name
    s3.upload_file("/tmp/"+file_name,"certificate-inventory",file_name)


def send_email_with_attachment(s):
    file_name=str(s)
    CHARSET = "utf-8"
    
# email header   
    msg = MIMEMultipart('mixed')
    msg['Subject'] = "Certificates expiring in 3 months"
    msg['From']="ankit@mail.ankittechi.tk"
    msg['To'] = "ankit.ex.kumar@accenture.com"
    #msg['Cc'] = "mechankit99@gmail.com"
    
    
    
    
# text based email body
    msg_body = MIMEMultipart('alternative')
    BODY_TEXT = "Hi Team,\n\nList of certificates which are expiring in 3 month has been uploaded to S3 bucket.\n\nBucket Name :- certificate-inventory\n\n"+"File Name :- "+file_name+"\n\nPlease find the attachement for required file.\n\nRegards,\nAnkit Kumar\nCloud Support Engineer\nAccenture Cloud Studio  |  ATCI\nCall : +919631127486\nCandor Tech space (SEZ), Sector-21, Gurgaon - 122001, INDIA"
    textpart = MIMEText(BODY_TEXT.encode(CHARSET), 'plain', CHARSET)
    msg_body.attach(textpart)
    
# Full path to the file that will be attached to the email.
    ATTACHMENT1="/tmp/"+file_name

# Adding attachments
    att1 = MIMEApplication(open(ATTACHMENT1, 'rb').read())
    att1.add_header('Content-Disposition', 'attachment',
                  filename=os.path.basename(ATTACHMENT1))

#Adding 
    msg.attach(msg_body)
    msg.attach(att1)
    try:
        response = ses_client.send_raw_email(
                RawMessage={
                    'Data': msg.as_string(),
                },
                ConfigurationSetName="ankit-ses-configset"
            
            )
        print("Message id : ", response['MessageId'])
        print("Message send successfully!")
    except Exception as e:
        print("Error: ", e)