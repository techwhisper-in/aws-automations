import boto3
import json
import pandas as pd
import sys
import datetime
import time
from awsglue.utils import getResolvedOptions
ssm=boto3.client('ssm')
sts=boto3.client('sts')
args = getResolvedOptions(sys.argv,['SNS_ARN','Bucket_Name','Account_Type','ARN_Lists','IAM_Arn','SSM_Acc_details'])
sns_topic = args['SNS_ARN'] # SNS topic to send notification
bucket_name = args['Bucket_Name'] # Bucket Name in which inventory file stored
account_type=args['Account_Type'] # Type of controller account Prod/Non-Prod/Preview/Dev/Test/Acceptance
ssm_name=args['ARN_Lists'] # ARNs of IAM role created for all other accounts
iam_arn=args['IAM_Arn'] # ARNs of IAM role created for all controller account
ssm_account_detail=args['SSM_Acc_details']
account_details=json.loads(ssm.get_parameter(Name=ssm_account_detail)['Parameter']['Value']) # Fetch details of account name and ids from parameter store
account_ids_list=account_details.keys() # list of account ids
now_date=str(datetime.datetime.now().date()) #current date
x=datetime.datetime.now()
f_suffix=str(x.strftime('%Y-%m-%d-%H-%M-%S'))
sns_list=[] # Defines variable to store details of identifiers for SNS service
loggroup_list=[] # Defines variable to store details of identifiers for Cloud Watch Log-Groups
cloudwatch_alarm_list=[] # Defines variable to store details of identifiers for Cloud Watch Alarms
event_rule_list=[] # Defines variable to store details of identifiers for Event Rules
lambda_function_list=[] # Defines variable to store details of identifiers for Lambda Function
sqs_list=[] # Defines variable to store details of identifiers for SQS
dynamodb_table_list=[] # Defines variable to store details of identifiers for Dynamodb
s3_list=[] # Defines variable to store details of identifiers for S3 bucket
glue_list=[] # Defines variable to store details of identifiers for Glue service
rds_list=[] # Defines variable to store details of identifiers for RDS
elasticsearch_domain_list=[] # Defines variable to store details of identifiers for ElasticSearch/Opensearch
kinesis_list=[] # Defines variable to store details of identifiers for Kinesis service
def fetch_inventory(event,context):
    rolearnlist_from_ssm=ssm.get_parameter(Name=ssm_name)
    rolearnlist=rolearnlist_from_ssm['Parameter']['Value'].split(",") # Stores IAM role of child and controller account
    region_name=['us-east-1','us-east-2']
    for x in rolearnlist:
        for reg in region_name:
            get_tags_all(x,reg) # Function call to fetch tag details of services other than S3
        get_tags_s3(x) # Function call to fetch tag details of S3
    excel_writer() # Function call to create excel file from fetched data
    upload_s3() # Function call to upload created excel file to S3 bucket
    notification_sns() # Function call for notification_sns for tag inventory

def tag_list(acc_id,acc_name,resource,item,service,region): # tag_list function define
    d={'Account Id':'#'+acc_id,'Account Name':acc_name,'Resource':resource,'Service':service,'Region':region}
    if str(type(item))=="<class 'list'>":
        for i in item:
            d[i['Key']]=i['Value']
    elif str(type(item))=="<class 'dict'>":
        d={**d,**item}
    if service=='SNS':
        sns_list.append(d)
    elif service=='Cloudwatch Log Group':
        loggroup_list.append(d)
    elif service=='Cloudwatch Alarm':
        cloudwatch_alarm_list.append(d)
    elif service=='Cloudwatch Event':
        event_rule_list.append(d)
    elif service=='Lambda Function':
        lambda_function_list.append(d)
    elif service=='SQS':
        sqs_list.append(d)
    elif service=='DynamoDB_Table':
        dynamodb_table_list.append(d)
    elif service=='S3':  
        s3_list.append(d)
    elif service=='Glue Jobs' or service=='Triggers' or service=='Crawlers':
        glue_list.append(d)
    elif service=='RDSDBInstance' or service=='RDSDBClusterSnapshot' or service=='RDSDBSnapshot':  
        rds_list.append(d)
    elif service=='ElasticSearch' or service=='OpenSearch':  
        elasticsearch_domain_list.append(d)
    elif service=='Firehose' or service=='KinesisDataStream':
        kinesis_list.append(d)

def get_tags_all(x,reg): # get_tags_all function define
    rolearn=x
    acc_id=str(x.split(":")[4])
    account_id='#'+acc_id
    if account_id in account_ids_list:
        acc_name=account_details[account_id]
    else:
        acc_name='Not Available'
    if rolearn==iam_arn:
    # <------Taking access for controller account---------->
        sns=boto3.client('sns',region_name=reg)
        log=boto3.client('logs',region_name=reg)
        alarm=boto3.client('cloudwatch',region_name=reg)
        event = boto3.client('events',region_name=reg)
        function = boto3.client('lambda',region_name=reg)
        sqs = boto3.client('sqs',region_name=reg)
        dynamodb = boto3.client('dynamodb',region_name=reg)
        glue=boto3.client('glue',region_name=reg)
        rds=boto3.client('rds',region_name=reg)
        es=boto3.client('es',region_name=reg)
        opensearch=boto3.client('opensearch',region_name=reg)
        firehose=boto3.client('firehose',region_name=reg)
        kinesis=boto3.client('kinesis',region_name=reg)
        print("Controller Account")
    #print(acc_id)
    else:
    # <------Taking access for one of the Target/Child account---------->
        awsaccount = sts.assume_role(
            RoleArn=rolearn,
            RoleSessionName='Glue_session_for_tags'
        )
        access_key = awsaccount['Credentials']['AccessKeyId']
        secret_key = awsaccount['Credentials']['SecretAccessKey']
        session_token = awsaccount['Credentials']['SessionToken']
        
        sns=boto3.client('sns',aws_access_key_id=access_key, aws_secret_access_key=secret_key, aws_session_token=session_token, region_name=reg)
        log=boto3.client('logs',aws_access_key_id=access_key, aws_secret_access_key=secret_key, aws_session_token=session_token, region_name=reg)
        alarm=boto3.client('cloudwatch',aws_access_key_id=access_key, aws_secret_access_key=secret_key, aws_session_token=session_token, region_name=reg)
        event = boto3.client('events',aws_access_key_id=access_key, aws_secret_access_key=secret_key, aws_session_token=session_token, region_name=reg)
        function = boto3.client('lambda',aws_access_key_id=access_key, aws_secret_access_key=secret_key, aws_session_token=session_token, region_name=reg)
        sqs = boto3.client('sqs',aws_access_key_id=access_key, aws_secret_access_key=secret_key, aws_session_token=session_token, region_name=reg)
        dynamodb = boto3.client('dynamodb',aws_access_key_id=access_key, aws_secret_access_key=secret_key, aws_session_token=session_token, region_name=reg)
        glue=boto3.client('glue',aws_access_key_id=access_key, aws_secret_access_key=secret_key, aws_session_token=session_token,region_name=reg)
        rds=boto3.client('rds',aws_access_key_id=access_key, aws_secret_access_key=secret_key, aws_session_token=session_token,region_name=reg)
        es=boto3.client('es',aws_access_key_id=access_key, aws_secret_access_key=secret_key, aws_session_token=session_token,region_name=reg)
        opensearch=boto3.client('opensearch',aws_access_key_id=access_key, aws_secret_access_key=secret_key, aws_session_token=session_token,region_name=reg)
        firehose=boto3.client('firehose',aws_access_key_id=access_key, aws_secret_access_key=secret_key, aws_session_token=session_token,region_name=reg)
        kinesis=boto3.client('kinesis',aws_access_key_id=access_key, aws_secret_access_key=secret_key, aws_session_token=session_token,region_name=reg)
        print("Target Account")
        
#kinesis
    print("Start Service Kinesis : "+acc_id+" "+reg)
    paginator = firehose.list_delivery_streams(Limit=123)
    for page in paginator['DeliveryStreamNames']:
        y=page
        try:
            response=firehose.list_tags_for_delivery_stream(DeliveryStreamName=y)
        except Exception as e:
            print(e)
            time.sleep(0.3)
            response=firehose.list_tags_for_delivery_stream(DeliveryStreamName=y)
        tag_list(acc_id,acc_name,y,response['Tags'],'Firehose',reg)

    paginator = kinesis.list_streams(Limit=123)
    for page in paginator['StreamNames']:
        y=page
        try:
            response=kinesis.list_tags_for_stream(StreamName=y)
        except Exception as e:
            print(e)
            time.sleep(0.3)
            response=kinesis.list_tags_for_stream(StreamName=y)
        tag_list(acc_id,acc_name,y,response['Tags'],'KinesisDataStream',reg)
    print("End Service Kinesis : "+acc_id+" "+reg)
#RDS
    print("Start Service RDS : "+acc_id+" "+reg)
    paginator = rds.get_paginator('describe_db_instances')
    for page in paginator.paginate():
        for j in range(len(page['DBInstances'])):
            y=str(page['DBInstances'][j]['DBInstanceArn'])
            try:
                response=rds.list_tags_for_resource(ResourceName=y)
            except Exception as e:
                print(e)
                time.sleep(0.3)
                response=rds.list_tags_for_resource(ResourceName=y)
            tag_list(acc_id,acc_name,y,response['TagList'],'RDSDBInstance',reg)

    paginator = rds.get_paginator('describe_db_cluster_snapshots')
    print(paginator)
    for page in paginator.paginate():
        for j in range(len(page['DBClusterSnapshots'])):
            y=str(page['DBClusterSnapshots'][j]['DBClusterSnapshotIdentifier'])
            try:
                response=rds.list_tags_for_resource(ResourceName='arn:aws:rds:'+reg+':'+acc_id+':cluster-snapshot:'+y)
            except Exception as e:
                print(e)
                time.sleep(0.3)
                response=rds.list_tags_for_resource(ResourceName='arn:aws:rds:'+reg+':'+acc_id+':cluster-snapshot:'+y)
            tag_list(acc_id,acc_name,y,response['TagList'],'RDSDBClusterSnapshot',reg)

    paginator = rds.get_paginator('describe_db_snapshots')
    for page in paginator.paginate():
        for j in range(len(page['DBSnapshots'])):
            y=str(page['DBSnapshots'][j]['DBSnapshotIdentifier'])
            try:
                response=rds.list_tags_for_resource(ResourceName='arn:aws:rds:'+reg+':'+acc_id+':snapshot:'+y)
            except Exception as e:
                print(e)
                time.sleep(0.3)
                response=rds.list_tags_for_resource(ResourceName='arn:aws:rds:'+reg+':'+acc_id+':snapshot:'+y)
            tag_list(acc_id,acc_name,y,response['TagList'],'RDSDBSnapshot',reg)
    print("End Service RDS : "+acc_id+" "+reg)
#SNS
    print("Start Service SNS : "+acc_id+" "+reg)
    paginator = sns.get_paginator('list_topics')
    for page in paginator.paginate():
        for j in range(len(page['Topics'])):
            y=str(page['Topics'][j]['TopicArn'])
            try:
                response=sns.list_tags_for_resource(ResourceArn=y)
            except Exception as e:
                print(e)
                time.sleep(0.3)
                response=sns.list_tags_for_resource(ResourceArn=y)
            tag_list(acc_id,acc_name,y.split(":")[5],response['Tags'],'SNS',reg)
    print("End Service SNS : "+acc_id+" "+reg)

#Opensearch/Elastisearch
    print("Start Service Opensearch/Elastisearch : "+acc_id+" "+reg)
    paginator = es.list_domain_names(EngineType='Elasticsearch')
    for i in paginator['DomainNames']:
        y=i['DomainName']
        try:
            response=es.list_tags(ARN='arn:aws:es:'+reg+':'+acc_id+':domain/'+i['DomainName'])
        except Exception as e:
            print(e)
            time.sleep(0.3)
            response=es.list_tags(ARN='arn:aws:es:'+reg+':'+acc_id+':domain/'+i['DomainName'])
        tag_list(acc_id,acc_name,y,response['TagList'],'ElasticSearch',reg)

    paginator = opensearch.list_domain_names(EngineType='OpenSearch')
    for i in paginator['DomainNames']:
        y=i['DomainName']
        try:
            response=es.list_tags(ARN='arn:aws:es:'+reg+':'+acc_id+':domain/'+i['DomainName'])
        except Exception as e:
            print(e)
            time.sleep(0.3)
            response=es.list_tags(ARN='arn:aws:es:'+reg+':'+acc_id+':domain/'+i['DomainName'])
        tag_list(acc_id,acc_name,y,response['TagList'],'OpenSearch',reg)
    print("End Service Opensearch/Elastisearch : "+acc_id+" "+reg)

#Cloudwatch log groups
    print("Start Service Cloudwatch log groups : "+acc_id+" "+reg)
    paginator = log.get_paginator('describe_log_groups')
    for page in paginator.paginate():
        for j in range(len(page['logGroups'])):
            y=str(page['logGroups'][j]['logGroupName'])
            #ar=str(page['logGroups'][j]['arn'])
            try:
                response=log.list_tags_log_group(logGroupName=y)
            except Exception as e:
                print(e)
                time.sleep(0.3)
                response=log.list_tags_log_group(logGroupName=y)
            tag_list(acc_id,acc_name,y,response['tags'],'Cloudwatch Log Group',reg)
    print("End Service Cloudwatch log groups : "+acc_id+" "+reg) 
#Cloudwatch alarm
    print("Start Service Cloudwatch alarm : "+acc_id+" "+reg)
    paginator = alarm.get_paginator('describe_alarms')
    for page in paginator.paginate():
        for j in range(len(page['MetricAlarms'])):
            y=str(page['MetricAlarms'][j]['AlarmName'])
            arn=str(page['MetricAlarms'][j]['AlarmArn'])
            try:
                response=alarm.list_tags_for_resource(ResourceARN=arn)
            except Exception as e:
                print(e)
                time.sleep(0.3)
                response=alarm.list_tags_for_resource(ResourceARN=arn)
            tag_list(acc_id,acc_name,y,response['Tags'],'Cloudwatch Alarm',reg)
    print("End Service Cloudwatch alarm : "+acc_id+" "+reg)
#Events
    print("Start Service Event rules : "+acc_id+" "+reg)
    paginator = event.get_paginator('list_rules')
    for page in paginator.paginate():
        for j in range(len(page['Rules'])):
            y=str(page['Rules'][j]['Name'])
            arn=str(page['Rules'][j]['Arn'])
            try:
                response = event.list_tags_for_resource(ResourceARN=arn)
            except Exception as e:
                print(e)
                time.sleep(0.3)
                response = event.list_tags_for_resource(ResourceARN=arn)
            tag_list(acc_id,acc_name,y,response['Tags'],'Cloudwatch Event',reg)
    print("End Service Event rules : "+acc_id+" "+reg)
#Lambda Function
    print("Start Service Lambda function : "+acc_id+" "+reg)
    paginator = function.get_paginator('list_functions')
    for page in paginator.paginate():
        for j in range(len(page['Functions'])):
            y=str(page['Functions'][j]['FunctionName'])
            arn=str(page['Functions'][j]['FunctionArn'])
            try:
                response = function.list_tags(Resource=arn)
            except Exception as e:
                print(e)
                time.sleep(0.3)
                response = function.list_tags(Resource=arn)
            tag_list(acc_id,acc_name,y,response['Tags'],'Lambda Function',reg)
    print("End Service Lambda function : "+acc_id+" "+reg)

#SQS
    print("Start Service SQS : "+acc_id+" "+reg)
    sqs_urls=[]
    queu=sqs.list_queues()
    if 'QueueUrls' in queu.keys():
        for i in queu['QueueUrls']:
            sqs_urls.append(i)
        #print(sqs_urls)
        for queue in sqs_urls:
            try:
                response = sqs.list_queue_tags(QueueUrl=queue)
            except Exception as e:
                print(e)
                time.sleep(0.3)
                response = sqs.list_queue_tags(QueueUrl=queue)
            if 'Tags' in response.keys():        
                tag_list(acc_id,acc_name,queue,response['Tags'],'SQS',reg)
            else:
                tag_list(acc_id,acc_name,queue,[{'Key':'','Value':''}],'SQS',reg)
    print("End Service SQS : "+acc_id+" "+reg)

#DynamoDB
    print("Start Service DynamoDB : "+acc_id+" "+reg)
    paginator = dynamodb.get_paginator('list_tables')
    for page in paginator.paginate():
        for y in page['TableNames']:
            response = dynamodb.describe_table(TableName=y)
            arn=response['Table']['TableArn']
            try:
                tag = dynamodb.list_tags_of_resource(ResourceArn=arn)
            except Exception as e:
                print(e)
                time.sleep(0.3)
                tag = dynamodb.list_tags_of_resource(ResourceArn=arn)
            tag_list(acc_id,acc_name,y,tag['Tags'],'DynamoDB_Table',reg)
    print("End Service DynamoDB : "+acc_id+" "+reg)

#GlueJob
    print("Start Service GlueJob : "+acc_id+" "+reg)
    paginator = glue.get_paginator('get_jobs')
    for page in paginator.paginate():
        for j in range(len(page['Jobs'])):
            y=str(page['Jobs'][j]['Name'])
            arn=f"arn:aws:glue:{reg}:{acc_id}:job/{y}"
            try:
                response=glue.get_tags(ResourceArn=arn)
            except Exception as e:
                print(e)
                time.sleep(0.3)
                response=glue.get_tags(ResourceArn=arn)
            tag_list(acc_id,acc_name,y,response['Tags'],'Glue Jobs',reg)

    paginator = glue.get_paginator('get_crawlers')
    for page in paginator.paginate():
        for j in range(len(page['Crawlers'])):
            y=str(page['Crawlers'][j]['Name'])
            arn=f"arn:aws:glue:{reg}:{acc_id}:crawler/{y}"
            try:
                response=glue.get_tags(ResourceArn=arn)
            except Exception as e:
                print(e)
                time.sleep(0.3)
                response=glue.get_tags(ResourceArn=arn)
            tag_list(acc_id,acc_name,y,response['Tags'],'Crawlers',reg)
            
    glue_trigger=glue.list_triggers(MaxResults=123)
    for i in glue_trigger['TriggerNames']:
        arn=f"arn:aws:glue:{reg}:{acc_id}:trigger/{i}"
        try:
            response=glue.get_tags(ResourceArn=arn)
        except Exception as e:
            print(e)
            time.sleep(0.3)
            response=glue.get_tags(ResourceArn=arn)
        tag_list(acc_id,acc_name,i,response['Tags'],'Triggers',reg)
    print("End Service Glue : "+acc_id+" "+reg)

def get_tags_s3(x): #get_tags_s3 function define
    rolearn=x
    acc_id=str(x.split(":")[4])
    print("Start Service S3 : "+acc_id)
    account_id='#'+acc_id
    if account_id in account_ids_list:
        acc_name=account_details[account_id]
    else:
        acc_name='Not Available'
    if rolearn==iam_arn:
    # <------Taking access for controller account---------->
        s3=boto3.client('s3')
        print("Controller Account")
    else:
    # <------Taking access for one of the Target/Child account---------->
        print("Target Account")
        awsaccount = sts.assume_role(
            RoleArn=rolearn,
            RoleSessionName='Glue_session_for_S3_tags'
        )
        access_key = awsaccount['Credentials']['AccessKeyId']
        secret_key = awsaccount['Credentials']['SecretAccessKey']
        session_token = awsaccount['Credentials']['SessionToken']
        s3=boto3.client('s3',aws_access_key_id=access_key, aws_secret_access_key=secret_key, aws_session_token=session_token)
    bucket_list=[]
    bucket=s3.list_buckets()
    #print(bucket)
    for i in bucket['Buckets']:
        n=i['Name']
        bucket_list.append(n)
    for j in bucket_list:
        reg=s3.get_bucket_location(Bucket=j)['LocationConstraint']
        if reg==None:
            reg='Global'
        else:
            reg=reg
        try:
            t=s3.get_bucket_tagging(Bucket=j)
            tag_list(acc_id,acc_name,j,t['TagSet'],'S3',reg)
        except Exception:
            tag_list(acc_id,acc_name,j,[{'Key':'','Value':''}],'S3',reg)
        #print(reg)
    print("End Service S3 : "+acc_id)

def excel_writer(): #excel_writer function define
    print("Start creating Excel")
    sns_data= pd.DataFrame(data=sns_list).fillna('(Not Tagged)') #pandas dataframe for SNS data
    loggroup_data= pd.DataFrame(data=loggroup_list).fillna('(Not Tagged)') #pandas dataframe for Clouwatch log group data
    cloudwatch_alarm_data= pd.DataFrame(data=cloudwatch_alarm_list).fillna('(Not Tagged)') #pandas dataframe for Cloudwatch Alarm data
    event_rule_data= pd.DataFrame(data=event_rule_list).fillna('(Not Tagged)') #pandas dataframe for Event Rule data
    lambda_function_data= pd.DataFrame(data=lambda_function_list).fillna('(Not Tagged)') #pandas dataframe for Lambda Function data
    sqs_data= pd.DataFrame(data=sqs_list).fillna('(Not Tagged)') #pandas dataframe for SQS data
    dynamodb_table_data= pd.DataFrame(data=dynamodb_table_list).fillna('(Not Tagged)') #pandas dataframe for DynamoDB data
    s3_data= pd.DataFrame(data=s3_list).fillna('(Not Tagged)') #pandas dataframe for S3 Bucket data
    glue_data=pd.DataFrame(data=glue_list).fillna('(Not Tagged)') #pandas dataframe for Glue service data
    rds_data=pd.DataFrame(data=rds_list).fillna('(Not Tagged)') #pandas dataframe for RDS data
    elasticsearch_domain_data=pd.DataFrame(data=elasticsearch_domain_list).fillna('(Not Tagged)') #pandas dataframe for ElasticSearch/OpenSearch data
    kinesis_data=pd.DataFrame(data=kinesis_list).fillna('(Not Tagged)') #pandas dataframe for Kinesis data
    #df1.fillna("Not Tagged")
    with pd.ExcelWriter('Tag_inventory.xlsx') as writer:
        sns_data.to_excel(writer, sheet_name='SNS',index=False)
        loggroup_data.to_excel(writer, sheet_name='Cloudwatch_log',index=False)
        cloudwatch_alarm_data.to_excel(writer, sheet_name='Cloudwatch_alarm',index=False)
        event_rule_data.to_excel(writer, sheet_name='Event_Rule',index=False)
        lambda_function_data.to_excel(writer, sheet_name='Lambda_function',index=False)
        sqs_data.to_excel(writer, sheet_name='SQS',index=False)
        dynamodb_table_data.to_excel(writer, sheet_name='DynamoDB_Table',index=False)
        s3_data.to_excel(writer, sheet_name='S3_Bucket',index=False)
        glue_data.to_excel(writer,sheet_name='Glue',index=False)
        rds_data.to_excel(writer,sheet_name='RDS',index=False)
        elasticsearch_domain_data.to_excel(writer,sheet_name='ElasticSearch-OpenSearch',index=False)
        kinesis_data.to_excel(writer,sheet_name='Kinesis-Firhose',index=False)
    print("Excel creation completed")
def upload_s3(): #upload_s3 function define
    print("Uploading Excel to S3")
    s3=boto3.client('s3')
    s3.upload_file('Tag_inventory.xlsx',bucket_name,'tag-inventory-files/Tag_inventory_'+account_type+'_'+f_suffix+'.xlsx')
    print("Excel file uploaded to S3")
def notification_sns(): #notification_sns function define
    print("Sending SNS Notification")
    sns=boto3.client('sns',region_name=sns_topic.split(":")[3])
    sns.publish(TopicArn=sns_topic,
    Subject="Fetched tags for "+account_type+" accounts || "+now_date,
    Message='Tags have been fetched. \n\n' +'Please check the S3 bucket: ' + bucket_name + '\n\nFolder Name:tag-inventory-files/' + '\n\nExcel File Name: Tag_inventory_'+account_type+'_'+f_suffix+'.xlsx')
    print("SNS sent")
fetch_inventory(str, str)