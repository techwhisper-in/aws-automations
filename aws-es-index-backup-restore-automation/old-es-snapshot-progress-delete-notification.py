import sys
import os
import boto3
import time
import json
import csv
import requests
from requests_aws4auth import AWS4Auth
from datetime import datetime,date
def lambda_handler(context,event):
    s3=boto3.client('s3')
    ssm=boto3.client('ssm',region_name=os.environ["Region_of_SSM_Parameter"])
    repo_name=ssm.get_parameter(Name=os.environ["SSM_Repository_Name"])
    name_of_snapshot_repo=repo_name['Parameter']['Value'] # snaphot repository name
    print(name_of_snapshot_repo)
    retention=ssm.get_parameter(Name=os.environ["SSM_Retention_Period"])
    retention_period=int(retention['Parameter']['Value']) #Retention Period of ES snapshot
    print(retention_period)
    BUCKET_NAME=os.environ["BUCKET_NAME"] #Bucket Name where snapshot stored
    es_host ="https://" + str(os.environ["Domain_Endpoint_Url"]) # AWS ES domain endpoint
    region =str(es_host.split('.')[1])  # region of AWS ES domain
   
    today = date.today()
    # Signing Requests
    service = 'es'
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
    headers = {"Content-Type": "application/json"}

#Delete snapshot code
    s = requests.get(es_host + '/_snapshot', auth=awsauth, headers=headers)
    dt=''
    repo=json.loads((s.text))
    k=list(repo.keys())
    for i in k[1:]:
        repo = requests.get(es_host + '/_snapshot/' + i + '/_all', auth=awsauth,headers=headers)
        snap=json.loads((repo.text))
        #print(snap)
        try:
            for j in range(len(snap['snapshots'])):
                snap_name=snap['snapshots'][j]['snapshot']
                name_of_the_snapshot=snap_name
                x=str(name_of_the_snapshot.split('_')[1])
                creation_date=datetime.strptime(x,"%d-%m-%y").date()
                snapshot_age=(today-creation_date).days
                print(name_of_the_snapshot,"   ",snapshot_age)
                if snapshot_age>retention_period:
                    delete_response=requests.delete(es_host + '/_snapshot/'+name_of_snapshot_repo+'/'+name_of_the_snapshot,auth=awsauth,headers=headers)
                    print(delete_response.text)
                    time.sleep(5)
                    if delete_response.text == "{\"acknowledged\":true}":
                        dt=dt+f"{name_of_the_snapshot} has been deleted from  repositotry {name_of_snapshot_repo} after {snapshot_age} Days of creation\n\n"
                        print(dt)
        except:
            print("No snapshot in this Repository")
    if dt=='':
        print("No Snapshot to delete")
        sub='''ES Index Snapshot deleted for cluster domain "'''+str(os.environ["Domain_Name"])+'''" || '''+str(today)+''' || '''+str(os.environ["Environment_Name"])
        sns=boto3.client('sns',region_name=region)
        sns.publish(TopicArn=os.environ["SNS_ARN"],
        Subject=sub,
        Message=f"No Snapshot to delete.\n\n{dt}")
    else:
        sub='''ES Index Snapshot deleted for cluster domain "'''+str(os.environ["Domain_Name"])+'''" || '''+str(today)+''' || '''+str(os.environ["Environment_Name"])
        sns=boto3.client('sns',region_name=region)
        sns.publish(TopicArn=os.environ["SNS_ARN"],
        Subject=sub,
        Message=f"Below Snapshot deleted.\n\n{dt}")
        
#ES notification code
    s = requests.get(es_host + '/_snapshot', auth=awsauth, headers=headers)
    repo=json.loads((s.text))
    k=list(repo.keys())
    a=[['Bucket Name','Repostery Name','Snapshot Name','Region']]
    KEY_NAME="Snapshots/repo_snap_in_"+BUCKET_NAME+".csv"
    with open('/tmp/snapshot.csv','w',encoding='UTF-8',newline='') as f:
        writer=csv.writer(f)
        writer.writerows(a)
        for i in k[1:]:
            repo = requests.get(es_host + '/_snapshot/' + i + '/_all', auth=awsauth,headers=headers)
            snap=json.loads((repo.text))
            #print(snap)
            try:
                for j in range(len(snap['snapshots'])):
                    snap_name=snap['snapshots'][j]['snapshot']
                    name_of_the_snapshot=snap_name
                    print(BUCKET_NAME,i,"  ",snap_name,region)
                    writer.writerows([[BUCKET_NAME,i,snap_name,region]])
            except:
                print(BUCKET_NAME,i,"   No snapshot in this repo",region)
                writer.writerows([[BUCKET_NAME,i,"No snapshot in this repository",region]])
        #writer.writerows 
    s3.upload_file('/tmp/snapshot.csv',BUCKET_NAME,KEY_NAME)

    r3=requests.get(es_host + '/_snapshot/' + name_of_snapshot_repo + '/_current',auth=awsauth,headers=headers)
    s=r3.text
    print(s)
    if "IN_PROGRESS" in s:
        print("In Progress")
        sub='''ES Index Snapshot for cluster domain "'''+str(os.environ["Domain_Name"])+'''" in Progress || '''+str(today)+''' || '''+str(os.environ["Environment_Name"])
        mes="Snapshot is in progress. Please find below details:\n\n"+s
    else:
        st=(requests.get(es_host + '/_snapshot/'+name_of_snapshot_repo+'/'+name_of_the_snapshot+'/_status',auth=awsauth,headers=headers)).text
        st=json.loads(st)['snapshots'][0]
        print(st)
        if (st['shards_stats']['failed'])==0:
            print("Completed")
            indice=st.pop('indices')
            indice=str(indice).replace("}}}},", "}}}},\n\n")
            sub='''ES Index Snapshot for cluster domain "'''+str(os.environ["Domain_Name"])+'''" taken || '''+str(today)+''' || '''+str(os.environ["Environment_Name"])
            mes=f"Index Snapshot has been completed. Please find below the details\n\n{st}\n\n{indice}"
        else:
            print("Failed")
            sub='''ES Index Snapshot for cluster domain "'''+str(os.environ["Domain_Name"])+'''" failed || '''+str(today)+''' || '''+str(os.environ["Environment_Name"])
            mes=f"Index Snapshot couldn't be completed. Please find below the details\n\n{st}"
    sns=boto3.client('sns',region_name=region)
    sns.publish(TopicArn=os.environ["SNS_ARN"],
    Subject=sub,
    Message=mes)
    #sys.exit()
#lambda_handler(str, str)
# EOF