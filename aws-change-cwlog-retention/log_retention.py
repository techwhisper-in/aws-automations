import boto3
import os
import time
sts=boto3.client('sts')
def lambda_handler(context,event):
    execution_role=os.environ['execution_role']
    sns_topic=os.environ['sns_topic']
    retention=int(os.environ['retention'])
    cross_account_iam=context['account_access_iams']
    rolearnlist=cross_account_iam.split(",")
    print(rolearnlist)
    for rolearn in rolearnlist:
        cross_account_access(rolearn,execution_role,sns_topic,retention)
def cross_account_access(rolearn,execution_role,sns_topic,retention):
    msg=[]
    ac='#'+rolearn.split(":")[4]
    ac_name=rolearn.split("-")[-2]
    ac_name_controller=execution_role.split("-")[-2]
    try:
        region_name=['us-east-1','ap-south-1']
        print(execution_role)
        print(rolearn)
        for reg in region_name:
            if rolearn==execution_role:
                client=boto3.client("logs",region_name=reg)
            else:
                print("Cross-Account-Session : ",ac)
                awsaccount = sts.assume_role(
                    RoleArn=rolearn,
                    RoleSessionName='awsaccount_session'
                )
                ACCESS_KEY = awsaccount['Credentials']['AccessKeyId']
                SECRET_KEY = awsaccount['Credentials']['SecretAccessKey']
                SESSION_TOKEN = awsaccount['Credentials']['SessionToken']
                client = boto3.client("logs",aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY, aws_session_token=SESSION_TOKEN, region_name=reg)
            paginator = client.get_paginator('describe_log_groups')
            for page in paginator.paginate():
                for j in range(len(page['logGroups'])):
                    if 'retentionInDays' not in page['logGroups'][j]:
                        try:
                            y=str(page['logGroups'][j]['logGroupName'])
                            response=client.list_tags_log_group(logGroupName=y)
                            log_details=str(str(ac)+"  "+str(y)+"   "+reg+"  "+str(response['tags'])+'\n')
                            response=client.put_retention_policy(logGroupName=y,retentionInDays=retention)
                            #msg.append(log_details)
                            if response['ResponseMetadata']['HTTPStatusCode']==200:
                                #mg={'AccountId':ac,'Name':str(y),'Region':reg,**response['tags']}
                                msg.append(log_details)
                        except Exception as e: #For handelling Throttling
                            print(e)
                            time.sleep(0.3)
                            y=str(page['logGroups'][j]['logGroupName'])
                            response=client.list_tags_log_group(logGroupName=y)
                            log_details=str(str(ac)+"  "+str(y)+"   "+reg+"  "+str(response['tags'])+'\n')
                            response=client.put_retention_policy(logGroupName=y,retentionInDays=retention)
                            #msg.append(log_details)
                            if response['ResponseMetadata']['HTTPStatusCode']==200:
                                #mg={'AccountId':ac,'Name':str(y),'Region':reg,**response['tags']}
                                msg.append(log_details)
                                print(ms)
        notification(msg,sns_topic,ac_name)
    except Exception as e:
        print(e)
        error_notification(e,sns_topic,ac_name,ac_name_controller)
def notification(msg,sns_topic,ac_name):
    m='\n'.join(map(str,msg))
    print(m)
    sns=boto3.client('sns',region_name=sns_topic.split(":")[3])
    sns.publish(TopicArn=sns_topic,
    Subject="Retention period of loggroups changed || "+ac_name,
    Message="Retention period of below loggroups changed:\n\n"+m)
    
def error_notification(e,sns_topic,ac_name,ac_name_controller):
    sns=boto3.client('sns',region_name=sns_topic.split(":")[3])
    sns.publish(TopicArn=sns_topic,
    Subject="Error Occuured in Lambda which changes Retention period of loggroups || "+ac_name_controller,
    Message="Error occurred for account "+ac_name+" and error logs are given below:\n\n"+str(e))