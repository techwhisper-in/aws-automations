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
    es_host ="https://" + str(os.environ["Domain_Endpoint_Url"]) # AWS ES domain endpoint
    region =str(es_host.split('.')[1])  # region of AWS ES domain
    flag = 0
    # defining variable for global reference later in the local function
    account_id = ""
    s3bucket_name = ""
    snapshot_rolename = ""

    # Signing Requests
    service = 'es'
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
    headers = {"Content-Type": "application/json"}


    # Manual Snapshot of whole cluster function:
    def snapshot_of_whole_cluster():
        global flag
        name_of_the_snapshot = "snap_"+date.today().strftime("%d-%m-%y")+"_"+datetime.now().strftime("%H%M%S") # snapshot_name
        r2 = requests.put(es_host + '/_snapshot/' + name_of_snapshot_repo + '/' + name_of_the_snapshot, auth=awsauth,
                        headers=headers)
        print("Manual Snapshot Creation Processing...")
        print(r2.text)
        r3=requests.get(es_host + '/_snapshot/' + name_of_snapshot_repo + '/_current',auth=awsauth,headers=headers)
        s=r3.text
        print(s)


        print(r2.text)
        if r2.text == "{\"accepted\":true}":
            print(name_of_the_snapshot + ": manual snapshot in progress!")
            print("Terminating Program!")
            
        else:
            print("Failure occurred during manual snapshot")


    def creating_snapshot_repo():
        s3bucket_name =os.environ["BUCKET_NAME"] # S3 Bucket In which snap to store
        time.sleep(10)
        snapshot_rolearn =os.environ["Role_Arn"] # Role arn to access s3 bucket
        
        ###################
        # Python Module to register manual snapshot registory (Avaialble in AWS Manual Snapshpot registory document)

        # Register repository
        path = '/_snapshot/' + name_of_snapshot_repo
        url = es_host + path
        print(url)

        payload = {
            "type": "s3",
            "settings": {
                "bucket": s3bucket_name,
                "region": region,
                "role_arn": snapshot_rolearn

            }
        }
        print(payload)

        r = requests.put(url, auth=awsauth, json=payload, headers=headers)
        print(r)
        # Validating Snapshot repository creation result

        if r.status_code == 200:
            print(name_of_snapshot_repo + " - snapshot repository has been registered successfully!")

            # Checking if there are any snapshots in process
            r1 = requests.get(es_host + '/_snapshot/_status', auth=awsauth, headers=headers)

            if r1.text == "{\"snapshots\":[]}":
                snapshot_of_whole_cluster()
            else:
                print(
                    "May be due to ongoing snapshots running program got terminated. The" + name_of_snapshot_repo + " snapshot repository has been created succefully. Hence, run the script once again after some time to take the manual snapshot.")
                sys.exit()  # terminating program
        else:
            print("Failure occurred in snapshot repository registration")

    # Checking if snapshot repository entered already exists
    s = requests.get(es_host + '/_snapshot', auth=awsauth, headers=headers)
    repo=json.loads((s.text))
    k=list(repo.keys())
    print(k)
    if name_of_snapshot_repo in k:
        user_choice = 'Y'
    #input("Entered snapshot repository already exists. Please type \'Y\' to continue taking manual snapshot OR type \'N\' to terminate program:")
        if user_choice == 'Y' or user_choice == 'y':
            flag = 1
            snapshot_of_whole_cluster()
        else:
            print("Terminating Program...")
            sys.exit()
    else:
        creating_snapshot_repo()



    #sys.exit()
#lambda_handler(str, str)
# EOF