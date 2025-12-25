import boto3
import csv
import datetime
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
s3=boto3.client('s3')
ssm=boto3.client('ssm')
sts=boto3.client('sts')
ses_client = boto3.client('ses')
li=['AccountId','EBS Volume Id','Service','Region','Size(In GB)','State']
l2=[]
iam_roles=os.environ['IAM_Parameter']
SNS_ARN=os.environ['SNS_ARN']
bucket_name=os.environ['BUCKET_NAME']
folder_name=os.environ['Folder_Name']
SES_From=os.environ['SES_From']
ACN_Recipients=os.environ['ACN_Recipients']
now_time=str(datetime.datetime.now().date())
x=datetime.datetime.now()
f_suffix=str(x.strftime('%Y-%m-%d-%H-%M-%S'))
def lambda_handler(event,context):
    for reg in ['ap-south-1']:
        accounts(reg)
    csv_writer()
    s3_upload()
    ses_attachement()
    notification()

def accounts(reg):
    rolearnlist_from_ssm=ssm.get_parameter(Name=iam_roles)
    rolearnlist=rolearnlist_from_ssm['Parameter']['Value'].split(",")
    for x in rolearnlist:
        get_volumes(reg,x)

def taglist(AccountId,volid,item,Service,Region,State,size):
    d={'AccountId':'#'+AccountId,'EBS Volume Id':volid,'Service':Service,'Region':Region,'Size(In GB)':size,'State':State}
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
    for i in list(d.keys()):
        if i not in li:
            li.append(i)
    l2.append(d)


def get_volumes(reg,rolearn):
    #ec2 = boto3.client('ec2', region_name=reg)
    AccountId=[rolearn.split(':')[4]]
    awsaccount = sts.assume_role(RoleArn=rolearn,RoleSessionName='awsaccount_session')
    ACCESS_KEY = awsaccount['Credentials']['AccessKeyId']
    SECRET_KEY = awsaccount['Credentials']['SecretAccessKey']
    SESSION_TOKEN = awsaccount['Credentials']['SessionToken']
    ec2 = boto3.client("ec2",aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY, aws_session_token=SESSION_TOKEN, region_name=reg )
    
    paginator = ec2.get_paginator('describe_volumes')
    for page in paginator.paginate():
        #print(page)
        #print("_______________")
        for j in range(len(page['Volumes'])):
            if page['Volumes'][j]['State']=='available':
                volid=page['Volumes'][j]['VolumeId']
                size=page['Volumes'][j]['Size']
                ch=page['Volumes'][j]['State']
                try:
                    tag=page['Volumes'][j]['Tags']
                except:
                    tag={}
                #print(volid,"  ",tag)
                taglist(AccountId[0],volid,tag,'Volumes',reg,'Available',size)

def csv_writer():
    fieldnames=li
    rows=l2
    with open('/tmp/get_volume.csv', 'w', encoding='UTF8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    with open('/tmp/get_volume.csv','r') as data:
        with open('/tmp/get_volumes.csv','w',encoding='UTF-8',newline='') as f:
            for line in csv.reader(data):
                for i in range(len(line)):
                    if line[i]=='':
                        line[i]='Not Tagged'
                #print(line)
                writer=csv.writer(f)
                writer.writerows([line])
    make_table() 
        
def make_table():
    ids=[]
    with open('/tmp/get_volumes.csv','r') as data:
        file=csv.DictReader(data)
        for col in file:
            if "Owner" in (col.keys()):
                ids.append(col['Owner'])
        ids=list(dict.fromkeys(ids))
        if "Not Tagged" in ids:
            ids.remove("Not Tagged")
        #print("IDS    ",ids)
        
    with open('/tmp/get_volumes.csv','r') as data:
        data=list(csv.reader(data))
        #print(data)
        #print(data[0])
        if "Owner" in data[0]:
            own_index=data[0].index('Owner')
            #print("Own index   ",own_index)
            for i in range(len(ids)):
                tr=''
                print(ids[i])
                for line in data:
                    if line[own_index]==str(ids[i]):
                        tr=tr+f'''<tr style=""><td style="border:solid windowtext 1.0pt; padding:.75pt .75pt .75pt .75pt"><p class="MsoNormal" align="center" style="margin: 0cm; font-size: 11pt; font-family: Calibri, sans-serif;text-align:center"><span style="">{line[0]}</span></p></td><td style="border:solid windowtext 1.0pt; padding:.75pt .75pt .75pt .75pt"><p class="MsoNormal" align="center" style="margin: 0cm; font-size: 11pt; font-family: Calibri, sans-serif;text-align:center"><span style="">{line[1]}</span></p></td><td style="border:solid windowtext 1.0pt; padding:.75pt .75pt .75pt .75pt"><p class="MsoNormal" align="center" style="margin: 0cm; font-size: 11pt; font-family: Calibri, sans-serif;text-align:center"><span style="">{line[2]}</span></p></td><td style="border:solid windowtext 1.0pt; padding:.75pt .75pt .75pt .75pt"><p class="MsoNormal" align="center" style="margin: 0cm; font-size: 11pt; font-family: Calibri, sans-serif;text-align:center"><span style="">{line[3]}</span></p></td><td style="border:solid windowtext 1.0pt; padding:.75pt .75pt .75pt .75pt"><p class="MsoNormal" align="center" style="margin: 0cm; font-size: 11pt; font-family: Calibri, sans-serif;text-align:center"><span style="">{line[4]}</span></p></td><td style="border:solid windowtext 1.0pt; padding:.75pt .75pt .75pt .75pt"><p class="MsoNormal" align="center" style="margin: 0cm; font-size: 11pt; font-family: Calibri, sans-serif;text-align:center"><span style="">{line[5]}</span></p></td><td style="border:solid windowtext 1.0pt; padding:.75pt .75pt .75pt .75pt"><p class="MsoNormal" align="center" style="margin: 0cm; font-size: 11pt; font-family: Calibri, sans-serif;text-align:center"><span style=""><a href="mailto:{line[own_index]}" data-auth="NotApplicable" data-loopstyle="link">{line[own_index]}</a></span></p></td></tr>'''
                send_email_with_table(ids[i],tr)
    if "Owner" in data[0]:
        with open('/tmp/unattached_ebs_volumes_without_owner-'+f_suffix+'.csv','w',encoding='UTF-8',newline='') as f:
            for line in data:
                if line[own_index]=='Owner' or line[own_index]=='Not Tagged':
                    writer=csv.writer(f)
                    writer.writerows([line])
    else:
        with open('/tmp/unattached_ebs_volumes_without_owner-'+f_suffix+'.csv','w',encoding='UTF-8',newline='') as f:
            for line in data:
                writer=csv.writer(f)
                writer.writerows([line])

def send_email_with_table(ids,tr):
    CHARSET = "utf-8"
    ids=ids
    #print(ids)
    y=tr+'''</tbody></table><div class="WordSection1"><p class="MsoNormal" style="margin: 0cm; font-size: 11pt; font-family: Calibri, sans-serif;">&nbsp;</p><p class="MsoNormal" style="margin: 0cm; font-size: 11pt; font-family: Calibri, sans-serif;">&nbsp;</p><p class="MsoNormal" style="margin: 0cm; font-size: 11pt; font-family: Calibri, sans-serif;"><span lang="EN-US" style="font-size:10.0pt; font-family:&quot;Graphik&quot;,sans-serif; color:#000099">Best Regards,</span></p><p class="MsoNormal" style="margin: 0cm; font-size: 11pt; font-family: Calibri, sans-serif;"><span lang="EN-US" style="color:#002060">ACN-Cloud Team</span></p><p class="MsoNormal" style="margin: 0cm; font-size: 11pt; font-family: Calibri, sans-serif;"><span lang="EN-US" style="color:#002060">'''+SES_From+'''</span></p><p class="MsoNormal" style="margin: 0cm; font-size: 11pt; font-family: Calibri, sans-serif;"><span lang="EN-US" style="font-size:10.0pt; font-family:&quot;Graphik&quot;,sans-serif; color:#A200FF">Auto-Generated Mail</span></p></div></body></html>'''
    x= """<html>
    <head>
    </head>
    <body><p>Hey,<br><br>As part of our continuous optimization process, we regularly monitor the usage of resources within our AWS environment. During our assessment, we have identified unattached EBS volumes that are no longer associated with any active EC2 instances. These "unattached" volumes contribute to unnecessary storage costs and resource inefficiency.<br><br>Please review the below list of unattached EBS volumes and let us know if these can be deleted:<br><br></p>
    <table class="MsoNormalTable" border="1" cellspacing="3" cellpadding="0" style="border:solid windowtext 1.0pt">
<tbody>
<tr style="">
<td style="border:solid windowtext 1.0pt; background:#90B6CF; padding:.75pt .75pt .75pt .75pt"><p class="MsoNormal" align="center" style="margin: 0cm; font-size: 11pt; font-family: Calibri, sans-serif;text-align:center"><span class="SpellE"><b><span style="color:black">AccountId</span></b></span><b><span style=""></span></b></p></td>
<td style="border:solid windowtext 1.0pt; background:#90B6CF; padding:.75pt .75pt .75pt .75pt"><p class="MsoNormal" align="center" style="margin: 0cm; font-size: 11pt; font-family: Calibri, sans-serif;text-align:center"><span class="SpellE"><b><span style="color:black">EBS Volume Id</span></b></span><b><span style=""></span></b></p></td>
<td style="border:solid windowtext 1.0pt; background:#90B6CF; padding:.75pt .75pt .75pt .75pt"><p class="MsoNormal" align="center" style="margin: 0cm; font-size: 11pt; font-family: Calibri, sans-serif;text-align:center"><b><span style="color:black">Service</span></b><b><span style=""></span></b></p></td>
<td style="border:solid windowtext 1.0pt; background:#90B6CF; padding:.75pt .75pt .75pt .75pt"><p class="MsoNormal" align="center" style="margin: 0cm; font-size: 11pt; font-family: Calibri, sans-serif;text-align:center"><b><span style="color:black">Region</span></b><b><span style=""></span></b></p></td>
<td style="border:solid windowtext 1.0pt; background:#90B6CF; padding:.75pt .75pt .75pt .75pt"><p class="MsoNormal" align="center" style="margin: 0cm; font-size: 11pt; font-family: Calibri, sans-serif;text-align:center"><b><span style="color:black">Size(<span class="SpellE">In GB</span>)</span></b><b><span style=""></span></b></p></td>
<td style="border:solid windowtext 1.0pt; background:#90B6CF; padding:.75pt .75pt .75pt .75pt"><p class="MsoNormal" align="center" style="margin: 0cm; font-size: 11pt; font-family: Calibri, sans-serif;text-align:center"><b><span style="color:black">State</span></b><b><span style=""></span></b></p></td>
<td style="border:solid windowtext 1.0pt; background:#90B6CF; padding:.75pt .75pt .75pt .75pt"><p class="MsoNormal" align="center" style="margin: 0cm; font-size: 11pt; font-family: Calibri, sans-serif;text-align:center"><b><span style="color:black">Owner</span></b><b><span style=""></span></b></p></td></tr>
"""+y

       
# email header   
    msg = MIMEMultipart('mixed')
    msg['Subject'] = "Action Required - Unattached EBS Volumes || "+now_time
    msg['From']=SES_From
    msg['To'] = ids
    msg['Cc'] = ACN_Recipients
    
    
    
    
# text based email body
    msg_body = MIMEMultipart('alternative')
    
    BODY_HTML = x
    #.format(**locals())

    htmlpart = MIMEText(BODY_HTML.encode(CHARSET), 'html', CHARSET)
    msg_body.attach(htmlpart)
    msg.attach(msg_body)
    #msg.attach(msg_body)
    #msg.attach(msg_body)


    try:
        response = ses_client.send_raw_email(
                RawMessage={
                    'Data': msg.as_string(),
                },
                #ConfigurationSetName="ses-configset"
            
            )
        print("Message id : ", response['MessageId'])
        print("Message send successfully!")
    except Exception as e:
        print("Error: ", e)

                
def ses_attachement():
    CHARSET = "utf-8"
    
# email header   
    msg = MIMEMultipart('mixed')
    msg['Subject'] = "Report for unattached EBS volumes without 'Owner' tag fetched || "+now_time
    msg['From']=SES_From
    #msg['To'] = ""
    msg['To'] = ACN_Recipients
    
# text based email body
    msg_body = MIMEMultipart('alternative')
    BODY_TEXT = f"Hi Team,\n\nDetails of un-attached EBS Volumes which don't have Owner tag have been uploaded to S3 bucket.\n\nBucket Name :- {bucket_name}\n\nFolder Name :- {folder_name}/ \n\nFile Name :- unattached_ebs_volumes_without_owner-{f_suffix}.csv\n\nPlease find the attachement and take appropriate action.\n\nRegards,\nACN-Cloud Team\n"+SES_From+"\nAuto Generated Mail"
    textpart = MIMEText(BODY_TEXT.encode(CHARSET), 'plain', CHARSET)
    msg_body.attach(textpart)
    
# Full path to the file that will be attached to the email.
    ATTACHMENT1="/tmp/unattached_ebs_volumes_without_owner-"+f_suffix+".csv"

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
                #ConfigurationSetName="ses-configset"
            
            )
        print("Message id : ", response['MessageId'])
        print("Message with attachement send successfully!")
    except Exception as e:
        print("Error: ", e)

def s3_upload():
    s3.upload_file('/tmp/unattached_ebs_volumes_without_owner-'+f_suffix+'.csv',bucket_name,folder_name+'/unattached_ebs_volumes_without_owner-'+f_suffix+'.csv')
    s3.upload_file('/tmp/get_volumes.csv',bucket_name,folder_name+'/unattached_ebs_volumes-'+f_suffix+'.csv')

def notification():
    sns=boto3.client('sns')
    sns.publish(TopicArn=SNS_ARN,
    Subject="Report for unattached EBS Volumes fetched || "+now_time,
    Message=f"Hi All,\n\nReport for unattached EBS Volumes fetched. Details are given below:\n\nS3 bucket :- {bucket_name} \n\nFolder Name :- {folder_name}/ \n\nCSV File Name :- unattached_ebs_volumes-{f_suffix}.csv")