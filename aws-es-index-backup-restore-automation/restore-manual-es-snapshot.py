import sys
import os
import boto3
import time
import json
import requests
from requests_aws4auth import AWS4Auth
def lambda_handler(context,event):
    print(context)
    events=context
    print(type(events))
    #s3=boto3.client('s3')
    #manual_snapshot_registort_code_inputs
    
    
    name_of_snapshot_repo=events['repository_name'] # snaphot repository name
    print(name_of_snapshot_repo)
    s3bucket_name =events['bucket_name'] # S3 Bucket In where snapshot is stored
    snapshot_rolearn =os.environ['Role_Arn'] # Role arn to access s3 bucket
    name_of_the_snapshot=events["snapshot_name"]
    restore_es_host=events["domain_endpoint_url"]
    index_name=events['index_name']
    suffix_to_restore_indices=events["suffix"]
    if restore_es_host[-1]=='/':
        restore_es_host=restore_es_host[0:-1]
    else:
        restore_es_host=restore_es_host
    region =str(restore_es_host.split('.')[1])  # region of AWS ES domain
    if suffix_to_restore_indices=='' and (index_name=='all' or index_name=='All'):
        payload = {"indices": "-.opendistro_security,-.kibana_1","include_global_state": False} #Replace kibana_sample_data_ecommerce with indexes name in comma separated formate.
    elif suffix_to_restore_indices!='' and (index_name=='all' or index_name=='All'):
        payload = {"indices": "-.opendistro_security,-.kibana_1","include_global_state": False,"rename_pattern":"(.+)","rename_replacement":"$1_"+suffix_to_restore_indices}
    elif suffix_to_restore_indices=='' and (index_name!='all' or index_name!='All'):
        payload = {"indices": index_name,"include_global_state": False}
    else:
        payload = {"indices": index_name,"include_global_state": False,"rename_pattern":"(.+)","rename_replacement":"$1_"+suffix_to_restore_indices}

    print(payload)

    # defining variable for global reference later in the local function
    #s3bucket_name = ""
    #snapshot_rolearn = ""

    # Signing Requests
    service = 'es'
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
    #
    headers = {"Content-Type": "application/json"}

    # Manual Snapshot of whole cluster function:
    def snapshot_of_whole_cluster(flag):
        flag=flag
        restore_choice = 'Y'
        if (restore_choice == 'Y' or restore_choice == 'y') and flag == 0:
            print("Creating the same snapshot repository named - " + name_of_snapshot_repo + "- for above ES domain")
            time.sleep(6)
            creating_snap_repo_for_restoreES(restore_es_host)
            print("Restoring in Process...")
            time.sleep(5)
            #delete_response = requests.delete(restore_es_host + '/_all')
            #print(delete_response)
            restore_response = requests.post(
                restore_es_host + '/_snapshot/' + name_of_snapshot_repo + '/' + name_of_the_snapshot + '/_restore',
                auth=awsauth,
                json=payload,
                headers=headers)
            print(restore_response.text)
            if restore_response.text == "{\"accepted\":true}":
                print("Snapshot successfully restored into ES domain: " + restore_es_host)
            else:
                print(restore_response.text)
                print("Restoring failed due to some error")
        elif (restore_choice == 'Y' or restore_choice == 'y') and flag == 1:
            print("Success!")
            print("Restoring in Process...")
            print(payload)
            time.sleep(5)
            #delete_response = requests.get(restore_es_host + '/_all')
            #print(delete_response)
            restore_response = requests.post(
                    restore_es_host + '/_snapshot/' + name_of_snapshot_repo + '/' + name_of_the_snapshot + '/_restore',
                    auth=awsauth,
                    json=payload,
                    headers=headers)
            print(restore_response.text)
            if restore_response.text == "{\"accepted\":true}":
                print("Snapshot successfully restored into ES domain: " + restore_es_host)
            else:
                print(restore_response.text)
                print("Restoring failed due to some error")
            
    def creating_snap_repo_for_restoreES(restore_es_host):
        # Register repository
        path = '/_snapshot/' + name_of_snapshot_repo
        url = restore_es_host + path
        payload = {
            "type": "s3",
            "settings": {
                "bucket": s3bucket_name,
                "region": region,
                "role_arn": snapshot_rolearn
            }
        }
        print(url)
        print(payload)
        r = requests.put(url, auth=awsauth, json=payload, headers=headers)
        if r.status_code == 200:
            print(
                name_of_snapshot_repo + " -snapshot repository has been registered successfully for ES domain where you want to restore the manual snapshot taken!")
        else:
            print(r.text)
            print("Failure occurred in snapshot repository creation for domain where you want to restore snapshot created!")
    # Checking if snapshot repository entered already exists
    s= requests.get(restore_es_host + '/_snapshot', auth=awsauth, headers=headers)
    repo=json.loads((s.text))
    k=list(repo.keys())
    print(k)
    if name_of_snapshot_repo in k:
        flag=1
        snapshot_of_whole_cluster(flag)
    elif name_of_snapshot_repo not in k:
        flag=0
        snapshot_of_whole_cluster(flag)
    else:
        print("Terminating Program...")
        sys.exit()
    # EOF
