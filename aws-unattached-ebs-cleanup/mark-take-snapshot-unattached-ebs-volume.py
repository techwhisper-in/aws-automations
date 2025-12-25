import boto3
from datetime import datetime,timezone
import csv
import os
#global variable define
current_time=datetime.now(timezone.utc)
current_date=datetime.today()
today_date=datetime.now().date()
prefix='/tmp/'
suffix=str(current_time.strftime('%Y-%m-%d-%H-%M-%S'))
f_name='ebs_marked_deletion_'+suffix+'.csv'
f_del_name='updated-ebs_volume_to_be_deleted.csv'
li=['AccountId','EBS Volume Id','Service','Region','Size(In GB)','State','SnapshotId','Event Days','User','Attached Instance','Event Time','Event_min']
l2=[]
#input values from Environment variable
s3_bucket=os.environ["BUCKET_NAME"]
s3_folder=os.environ["Folder_Name"]
s3_folder_marked=s3_folder+"marked_deletion_data/"
sns_topic =os.environ["SNS_ARN"]
aging=int(os.environ["Aging"])
iam_parameter_store=os.environ["IAM_Parameter"]
env=os.environ["Environment"]
lambda_role=os.environ["IAM_Role"]
#service access
sts=boto3.client('sts')
s3=boto3.client('s3')
ssm=boto3.client('ssm')
def lambda_handler(event,context): #main function
    for reg in ['ap-south-1']:
        accounts_session(reg,aging)
    csv_writer(li,l2)
    upload_file_s3()
    notification()
def accounts_session(reg,aging):          #Function to account sessions
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
        get_volumes(AccountId,reg,ec2,cloudtrail,aging)

def taglist(item,d):     #Function to change key-value pair of tags to dictionary-list
    for i in item:
        d[i['Key']]=i['Value']
        Keys=(list(d.keys()))
        for i in Keys:
            if i not in li:
                li.append(i)
    return(d)
def unattached_aging_ebs(Vol,cloudtrail,aging):     #Function to calculate age of unattached EBS volumes
    vol_trail=[]
    paginator = cloudtrail.get_paginator('lookup_events')
    response_iterator = paginator.paginate(
        LookupAttributes=[
            {
                'AttributeKey': 'ResourceName',
                'AttributeValue': Vol
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
            'AttributeValue': Vol
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
def create_ebs_snapshot(ec2,volume_id,tg):    #Function to initiate snapshot of unattached EBS volume
    #print(tg)
    if len(tg)==1 and tg[0]['Key']=='':
        take_snap = ec2.create_snapshot(
        Description=f'Snapshot of {volume_id} on {today_date} before automatic deletion',
        VolumeId=volume_id,
        )
    else:
        take_snap = ec2.create_snapshot(
        Description=f'Snapshot of {volume_id} on {today_date} before automatic deletion',
        VolumeId=volume_id,
        TagSpecifications=[
        {
            'ResourceType':'snapshot',
            'Tags': tg
        },
        ],)
    #print(take_snap)
    try:
        snap={'SnapshotId':take_snap.id}
    except:
        snap={'SnapshotId':take_snap['SnapshotId']}
    return(snap)
def get_volumes(AccountId,reg,ec2,cloudtrail,aging):       #Function to get list of unattached EBS volumes
    paginator = ec2.get_paginator('describe_volumes')
    for page in paginator.paginate(Filters=[{'Name': 'status', 'Values': ['available']}]):
        #print(page)
        for v in page['Volumes']:
            #if v['State']=='available':
            volid=v['VolumeId']
            size=v['Size']
            state=v['State']
            data1={'AccountId':AccountId,'EBS Volume Id':volid,'Service':'EBS Volumes','Region':reg,'Size(In GB)':size,'State':state}
            tg=v.get('Tags',[{'Key': '', 'Value': ''}])
            tag=taglist(tg,{})
            if tag.get('Delete'):
                if (tag['Delete'] not in ['No','no','NO']):
                    ebs_trail=unattached_aging_ebs(volid,cloudtrail,aging)
                    if str(type(ebs_trail))=="<class 'list'>" and (ebs_trail[0]=="Aging not fulfilled"):
                        print("Aging not fulfilled for below:\n"+str({**data1,**ebs_trail[1],**tag})+"\n\n")
                    elif (ebs_trail=="No CloudTrail available") or (ebs_trail=='No DetachVolume event available'):
                        creation_time=v['CreateTime']
                        age=int((current_time-creation_time).days)
                        if age>aging:
                            snapid=create_ebs_snapshot(ec2,volid,tg)
                            dr={**data1,**snapid,**{'Event Days': 'No Trail', 'User': 'No Trail', 'Attached Instance': 'No Trail', 'Event Time': 'No Trail', 'Event_min': 'No Trail'},**tag}
                            l2.append(dr)
                        else:
                            print(f"No trail available and Creation age is less than {aging}. Current creation age is {age} for below:\n{str({**data1,**{'Event Days': 'No Trail', 'User': 'No Trail', 'Attached Instance': 'No Trail', 'Event Time': 'No Trail', 'Event_min': 'No Trail'},**tag})}\n\n")
                    elif str(type(ebs_trail))=="<class 'dict'>":
                        snapid=create_ebs_snapshot(ec2,volid,tg)
                        dr={**data1,**snapid,**ebs_trail,**tag}
                        l2.append(dr)
                else:
                    print(f"The {volid} is having exception from tag: {tag}")
            else:
                ebs_trail=unattached_aging_ebs(volid,cloudtrail,aging)
                if str(type(ebs_trail))=="<class 'list'>" and (ebs_trail[0]=="Aging not fulfilled"):
                    print("Aging not fulfilled for below:\n"+str({**data1,**ebs_trail[1],**tag})+"\n\n")
                elif (ebs_trail=="No CloudTrail available") or (ebs_trail=='No DetachVolume event available'):
                    creation_time=v['CreateTime']
                    age=int((current_time-creation_time).days)
                    if age>aging:
                        snapid=create_ebs_snapshot(ec2,volid,tg)
                        dr={**data1,**snapid,**{'Event Days': 'No Trail', 'User': 'No Trail', 'Attached Instance': 'No Trail', 'Event Time': 'No Trail', 'Event_min': 'No Trail'},**tag}
                        l2.append(dr)
                    else:
                        print(f"No trail available and Creation age is less than {aging}. Current creation age is {age} for below:\n{str({**data1,**{'Event Days': 'No Trail', 'User': 'No Trail', 'Attached Instance': 'No Trail', 'Event Time': 'No Trail', 'Event_min': 'No Trail'},**tag})}\n\n")
                elif str(type(ebs_trail))=="<class 'dict'>":
                    snapid=create_ebs_snapshot(ec2,volid,tg)
                    dr={**data1,**snapid,**ebs_trail,**tag}
                    l2.append(dr)
def csv_writer(li,l2):       #Function to create csv file contains data of EBS volumes marked for deletion
    fieldnames=li
    data=l2
    with open(prefix+'temp_sheet1'+suffix+'.csv', 'w', encoding='UTF8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    with open(prefix+'temp_sheet1'+suffix+'.csv','r') as datas:
        with open(prefix+'delete_ebs_'+suffix+'.csv','w',encoding='UTF-8',newline='') as f:
            for line in csv.reader(datas):
                for i in range(len(line)):
                    if line[i]=='':
                        line[i]='Not Tagged'
                writer=csv.writer(f)
                writer.writerows([line])
def upload_file_s3():          #Function to upload CSV file to S3 bucket
    s3.upload_file(prefix+'delete_ebs_'+suffix+'.csv',s3_bucket,s3_folder_marked+f_name)
    s3.upload_file(prefix+'delete_ebs_'+suffix+'.csv',s3_bucket,s3_folder+f_del_name)
def notification():           #Function to send SNS notification
    sns=boto3.client('sns',region_name=sns_topic.split(':')[3])
    sns.publish(TopicArn=sns_topic,
    Subject=f"Unattached EBS volume marked for deletion || {env} || {today_date}",
    Message=f"Unattached EBS volume marked for deletion.\n\nPlease check S3 bucket :- {s3_bucket}\n\n Go to below folder/file to get CSV file which contains details of unattached EBS volumes marked for deletion and will be deleted next day:\n   • Folder name:- {s3_folder_marked}\n   • File Name:- {f_name}\n\n Go to below folder/file to get CSV file which reads details of unattached EBS volumes before deleting those next day:\n   • Folder Name:- {s3_folder}\n   • File Name:- {f_del_name}")
