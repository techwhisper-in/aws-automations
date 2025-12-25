import boto3
from datetime import datetime,timezone
import csv
import os
current_time=datetime.now(timezone.utc)
current_date=datetime.today()
today_date=datetime.now().date()
prefix='/tmp/'
suffix=str(current_time.strftime('%Y-%m-%d-%H-%M-%S'))
li=['AccountId','EBS Volume Id','Service','Region','Size(In GB)','State','SnapshotId','Event Days','User','Attached Instance','Event Time','Event_min']
l2=[]
f_name='deleted_unattached_ebs_volumes_'+suffix+'.csv'
f_del_name='updated-ebs_volume_to_be_deleted.csv'
s3_bucket=os.environ["BUCKET_NAME"]
s3_folder=os.environ["Folder_Name"]
s3_folder_deleted=s3_folder+"deleted_data/"
sns_topic =os.environ["SNS_ARN"]
aging=int(os.environ["Aging"])
iam_parameter_store=os.environ["IAM_Parameter"]
env=os.environ["Environment"]
lambda_role=os.environ["IAM_Role"]
sts=boto3.client('sts')
s3=boto3.client('s3')
ssm=boto3.client('ssm')
def lambda_handler(event,context):
    vol_data=download_vol_data_from_s3()
    for reg in ['ap-south-1','us-east-1']:
        accounts_session(reg,aging,vol_data)
    csv_writer(li,l2)
    upload_file_s3()
    notification()
def download_vol_data_from_s3():
    s3 = boto3.client('s3')
    bucket_name = s3_bucket  # replace with your bucket name
    file_key = s3_folder+f_del_name  # replace with your file key
    download_path = '/tmp/ebs_volume_to_be_deleted.csv'
    vol_data=[]
    try:
        s3.download_file(bucket_name, file_key, download_path)
        with open(download_path, 'r') as del_data:
            li_d=csv.DictReader(del_data)  
            for row in li_d:
                vol_data.append(row)
        return(vol_data)
    except Exception as e:
        print(e)
        raise e
def accounts_session(reg,aging,vol_data):          #Function to account sessions
    rolearnlist_from_ssm=ssm.get_parameter(Name=iam_parameter_store)
    rolearnlist=rolearnlist_from_ssm['Parameter']['Value'].split(",")
    for rolearn in rolearnlist:
        AccountId='#'+str(([rolearn.split(':')[4]])[-1])
        if rolearn==lambda_role:
            ec2=boto3.client("ec2",region_name=reg)
            cloudtrail = boto3.client('cloudtrail',region_name=reg)
        else:
            awsaccount = sts.assume_role(RoleArn=rolearn,RoleSessionName='awsaccount_session')
            ACCESS_KEY = awsaccount['Credentials']['AccessKeyId']
            SECRET_KEY = awsaccount['Credentials']['SecretAccessKey']
            SESSION_TOKEN = awsaccount['Credentials']['SessionToken']
            ec2 = boto3.client("ec2",aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY, aws_session_token=SESSION_TOKEN, region_name=reg)
            cloudtrail = boto3.client('cloudtrail',aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY, aws_session_token=SESSION_TOKEN, region_name=reg)
        for i in vol_data:
            if i['AccountId']==AccountId and i['Region']==reg:
                check_snapshot_volume_status(i,aging,ec2,cloudtrail)
def check_snapshot_volume_status(del_data,aging,ec2,cloudtrail):
    snap_response=ec2.describe_snapshots(SnapshotIds=[del_data['SnapshotId']])
    if snap_response['Snapshots'][0]['State']=='completed' and snap_response['Snapshots'][0]['Progress']=='100%':
        vol_response=ec2.describe_volumes(VolumeIds=[del_data['EBS Volume Id']])
        volid=del_data['EBS Volume Id']
        size=vol_response['Volumes'][0]['Size']
        state=vol_response['Volumes'][0]['State']
        region=vol_response['Volumes'][0]['AvailabilityZone'][:-1]
        snapid={'SnapshotId':del_data['SnapshotId']}
        if state=='available':
            data1={'AccountId':del_data['AccountId'],'EBS Volume Id':volid,'Service':'EBS Volumes','Region':region,'Size(In GB)':size,'State':state}
            tg=vol_response['Volumes'][0].get('Tags',[{'Key': '', 'Value': ''}])
            tag=taglist(tg,{})
            if tag.get('Delete'):
                if (tag['Delete'] not in ['No','no','NO']):
                    ebs_trail=unattached_aging_ebs(volid,cloudtrail,aging)
                    if str(type(ebs_trail))=="<class 'list'>" and (ebs_trail[0]=="Aging not fulfilled"):
                        print("Aging not fulfilled for below:\n"+str({**data1,**ebs_trail[1],**tag})+"\n\n")
                    elif (ebs_trail=="No CloudTrail available") or (ebs_trail=='No DetachVolume event available'):
                        creation_time=vol_response['Volumes'][0]['CreateTime']
                        age=int((current_time-creation_time).days)
                        if age>aging:
                            del_response=delete_unattached_ebs(ec2,volid)
                            dr={**data1,**snapid,**{'Event Days': 'No Trail', 'User': 'No Trail', 'Attached Instance': 'No Trail', 'Event Time': 'No Trail', 'Event_min': 'No Trail'},**tag}
                            l2.append(dr)
                        else:
                            print(f"No trail available and Creation age is less than {aging}. Current creation age is {age} for below:\n{str({**data1,**{'Event Days': 'No Trail', 'User': 'No Trail', 'Attached Instance': 'No Trail', 'Event Time': 'No Trail', 'Event_min': 'No Trail'},**tag})}\n\n")
                    elif str(type(ebs_trail))=="<class 'dict'>":
                        del_response=delete_unattached_ebs(ec2,volid)
                        dr={**data1,**snapid,**ebs_trail,**tag}
                        l2.append(dr)
                else:
                    print(f"The {volid} is having exception from tag: {tag}")
            else:
                ebs_trail=unattached_aging_ebs(volid,cloudtrail,aging)
                if str(type(ebs_trail))=="<class 'list'>" and (ebs_trail[0]=="Aging not fulfilled"):
                    print("Aging not fulfilled for below:\n"+str({**data1,**ebs_trail[1],**tag})+"\n\n")
                elif (ebs_trail=="No CloudTrail available") or (ebs_trail=='No DetachVolume event available'):
                    creation_time=vol_response['Volumes'][0]['CreateTime']
                    age=int((current_time-creation_time).days)
                    if age>aging:
                        del_response=delete_unattached_ebs(ec2,volid)
                        dr={**data1,**snapid,**{'Event Days': 'No Trail', 'User': 'No Trail', 'Attached Instance': 'No Trail', 'Event Time': 'No Trail', 'Event_min': 'No Trail'},**tag}
                        l2.append(dr)
                    else:
                        print(f"No trail available and Creation age is less than {aging}. Current creation age is {age} for below:\n{str({**data1,**{'Event Days': 'No Trail', 'User': 'No Trail', 'Attached Instance': 'No Trail', 'Event Time': 'No Trail', 'Event_min': 'No Trail'},**tag})}\n\n")
                elif str(type(ebs_trail))=="<class 'dict'>":
                    del_response=delete_unattached_ebs(ec2,volid)
                    dr={**data1,**snapid,**ebs_trail,**tag}
                    l2.append(dr)
def taglist(item,d):     #Function to change key-value pair of tags to dictionary-list
    for j in item:
        d[j['Key']]=j['Value']
        Keys=(list(d.keys()))
        for j in Keys:
            if j not in li:
                li.append(j)
    return(d)
def unattached_aging_ebs(volid,cloudtrail,aging):
    vol_trail=[]
    paginator = cloudtrail.get_paginator('lookup_events')
    response_iterator = paginator.paginate(
        LookupAttributes=[
            {
                'AttributeKey': 'ResourceName',
                'AttributeValue': volid
            },
        ],
        PaginationConfig={
            'MaxItems': 123,
            'PageSize': 123,
        }
    )
    for page in response_iterator:
        if len(page['Events'])==0:
            return("No CloudTrail available")
        else:
            for i in page['Events']:
                #print(i)
                if i.get('EventName','Other')=='DetachVolume':
                    ev_time=(i.get('EventTime','Not Available'))
                    event_minutes=((current_time-ev_time).total_seconds())/60
                    event_days=(current_time-ev_time).days
                    user=i.get('Username','Not Available')
                    resource=i.get('Resources',['Not Available'])
                    for r in resource:
                        if r.get('ResourceType','Not Available')=='AWS::EC2::Volume':
                            volume_id=r.get('ResourceName','Not Available')
                        elif r.get('ResourceType','Not Available')=='AWS::EC2::Instance':
                            attached_instance=r.get('ResourceName','Not Available')
                    vol_trail.append({'Event Days':event_days,'User':user,'Attached Instance':attached_instance,'Event Time':str(ev_time),'Event_min':event_minutes})
        item=page.get('NextToken','Not Available')
        #print(item)
    while (str(item)!='Not Available'):
        paginator = cloudtrail.get_paginator('lookup_events')
        response_iterator = paginator.paginate(
            LookupAttributes=[
                {
                    'AttributeKey': 'ResourceName',
            'AttributeValue': volid
        },
            ],

            PaginationConfig={
                'MaxItems': 123,
                'PageSize': 123,
                'StartingToken': item
            }
        )
        #print('\n\nFrom Next Token')
        for page in response_iterator:
            item=page.get('NextToken','Not Available')
            for i in page['Events']:
                #print(i)
                if i.get('EventName','Other')=='DetachVolume':
                    ev_time=(i.get('EventTime','Not Available'))
                    event_minutes=((current_time-ev_time).total_seconds())/60
                    event_days=(current_time-ev_time).days
                    user=i.get('Username','Not Available')
                    resource=i.get('Resources',['Not Available'])
                    for r in resource:
                        if r.get('ResourceType','Not Available')=='AWS::EC2::Volume':
                            volume_id=r.get('ResourceName','Not Available')
                        elif r.get('ResourceType','Not Available')=='AWS::EC2::Instance':
                            attached_instance=r.get('ResourceName','Not Available')
                        #if event_days>aging:
                    vol_trail.append({'Event Days':event_days,'User':user,'Attached Instance':attached_instance,'Event Time':str(ev_time),'Event_min':event_minutes})
    #print(vol_trail)
    volu='No DetachVolume event available'
    ev_min=131040
    for i in vol_trail:
        if i.get('Event_min','NA')<ev_min:
            ev_min=i.get('Event_min','NA')
            volu=i
    if str(type(volu))=="<class 'dict'>":
        if volu['Event Days']>aging:
            return(volu)
        else:
            return(["Aging not fulfilled",volu])
    else:
        return volu




#Only left work---------------------------------------------####


def delete_unattached_ebs(ec2,volid):
    print(volid)









#Only left work---------------------------------------------####


def csv_writer(li,l2):       #Function to create csv file contains data of EBS volumes marked for deletion
    fieldnames=li
    data=l2
    with open(prefix+'temp_sheet1'+suffix+'.csv', 'w', encoding='UTF8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    with open(prefix+'temp_sheet1'+suffix+'.csv','r') as datas:
        with open(prefix+'deleted_ebs_'+suffix+'.csv','w',encoding='UTF-8',newline='') as f:
            for line in csv.reader(datas):
                for i in range(len(line)):
                    if line[i]=='':
                        line[i]='Not Tagged'
                writer=csv.writer(f)
                writer.writerows([line])
def upload_file_s3():          #Function to upload CSV file to S3 bucket
    s3.upload_file(prefix+'deleted_ebs_'+suffix+'.csv',s3_bucket,s3_folder_deleted+f_name)
def notification():           #Function to send SNS notification
    sns=boto3.client('sns',region_name=sns_topic.split(':')[3])
    sns.publish(TopicArn=sns_topic,
    Subject=f"Unattached EBS volumes deleted || {env} || {today_date}",
    Message=f"Unattached EBS volumes have been deleted.\n\nPlease check S3 bucket :- {s3_bucket}\n\n Go to below folder/file to get CSV file which contains details of deleted unattached EBS volumes:\n   • Folder name:- {s3_folder_deleted}\n   • File Name:- {f_name}\n\n")
