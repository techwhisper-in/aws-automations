import pandas as pd
import boto3
import os
import assume_role
import time

iam_client = boto3.client('iam')
ssm_client = boto3.client('ssm')
sns_client = boto3.client('sns')
lambda_client = boto3.client('lambda')
sns_client = boto3.client('sns')
sqs_client = boto3.client('sqs')
events_client = boto3.client('events')
s3_client = boto3.client('s3')
s3_res = boto3.resource('s3')
sts_client = boto3.client('sts')
cwlog_client = boto3.client('logs')
ddb_client = boto3.client('dynamodb')
cwalarm_client = boto3.client('cloudwatch')

rolearn_current= 'arn:aws:iam::494829558485:role/ankit-dvmon1-glue-taginventory'

def s3_describe(account_name='controller-account', account_id= sts_client.get_caller_identity()['Account'], rolearn=rolearn_current):
    
    bucket_list = []
    tag_out ={}
    ls_out = []
    
    if rolearn != rolearn_current:
        client = assume_role.client_func(rolearn, 's3')
    else:
        client = s3_client
    
    res = client.list_buckets (
        )
    for i in res['Buckets']:
        n = i['Name']
        bucket_list.append(n)
    
    for j in bucket_list:
        value_cost=''
        value_env=''
        value_name=''
        value_createdby = ''
        value_approle = ''
        value_lob = ''
        value_app = ''
        value_comp = ''
    
        try:
            response = client.get_bucket_tagging(
                Bucket= j
                )
            tag_list = response['TagSet']
        
            for i in range(len(tag_list)):
                if tag_list[i]['Key'] == 'Name':
                    value_name = tag_list[i]['Value']
                if tag_list[i]['Key'] == 'environment':
                    value_env = tag_list[i]['Value']
                if tag_list[i]['Key'] == 'costcenter':
                    value_cost = tag_list[i]['Value']
                if tag_list[i]['Key'] == 'createdby':
                    value_createdby = tag_list[i]['Value']
                if tag_list[i]['Key'] == 'applicationrole':
                    value_approle = tag_list[i]['Value']
                if tag_list[i]['Key'] == 'lob':
                    value_lob = tag_list[i]['Value']
                if tag_list[i]['Key'] == 'application':
                    value_app = tag_list[i]['Value']
                if tag_list[i]['Key'] == 'compliance':
                    value_comp = tag_list[i]['Value']
            
            tag_out = [{"Account Name":account_name,"Account ID":account_id,"Bucket Name": j,"Name" : value_name, "environment": value_env, "costcenter":value_cost, "createdby":value_createdby,"applicationrole":value_approle,"application":value_app,"compliance":value_comp,"lob":value_lob}]
            
            for i in tag_out:
                ls_out.append(i)
        
        except:
            tag_out = [{"Account Name":account_name,"Account ID":account_id,"Bucket Name": j, "Name" : '', "environment": '', "costcenter":'',"createdby":'',"applicationrole":'',"application":'',"compliance":'',"lob":''}]
            
            for i in tag_out:
                ls_out.append(i)
                
    df = pd.DataFrame(ls_out)
    
    return df

def lambda_desribe(account_name='controller-account', account_id= sts_client.get_caller_identity()['Account'],rolearn=rolearn_current):
    functions_list = []
    functions_name = []
    ls_list_lambda= []
    tag_list = {}
    
    if rolearn != rolearn_current:
        client = assume_role.client_func(rolearn, 'lambda')
    else:
        client = lambda_client
   
    response = client.list_functions()
    for i in response['Functions']:
        functions_list.append(i['FunctionArn'])
        functions_name.append(i['FunctionName'])

    while 'NextMarker' in response.keys():
        response = client.list_functions(Marker=response['NextMarker'])
        for i in response['Functions']:
            functions_list.append(i['FunctionArn'])
            functions_name.append(i['FunctionName'])
        time.sleep(1)


    for i in functions_name:
        tag_name = ""
        tag_env = ""
        value_createdby= ''
        tag_costcenter=''
        tag_approle=''
        value_app=''
        value_comp=''
        value_lob=''
        Functionname = i
        tags = client.get_function(FunctionName=i)
        if 'Tags' in tags:
            b =  tags['Tags']
            for i in b:
                if i == "Name":
                    tag_name = b['Name']
                if i == "applicationrole":
                    tag_approle = b['applicationrole']
                if i == "costcenter":
                    tag_costcenter = b['costcenter']
                if i == "environment":
                    tag_env = b['environment']
                if i == 'createdby':
                    value_createdby = b['createdby']
                if i == 'application':
                    value_app = b['application']
                if i == 'lob':
                    value_lob = b['lob']
                if i == 'compliance':
                    value_comp = b['compliance']
            
        tag_list = [{"Account Name":account_name,"Account ID":account_id,'Function Name': Functionname, 'Name':tag_name, 'environment':tag_env,"costcenter":tag_costcenter, "createdby":value_createdby,"applicationrole":tag_approle,"application":value_app,"compliance":value_comp,"lob":value_lob}]
        for j in tag_list:
            ls_list_lambda.append(j)
        
    df = pd.DataFrame(ls_list_lambda)
    
    return df

def sns_describe(account_name='controller-account', account_id= sts_client.get_caller_identity()['Account'],rolearn=rolearn_current):
    tag_list = []
    tag_out ={}
    ls_out = []
    snstopicarn = []
    
    if rolearn != rolearn_current:
        client = assume_role.client_func(rolearn, 'sns')
    else:
        client = sns_client
    
    res = client.list_topics (
        )
    for i in res['Topics']:
        snstopicarn.append(i['TopicArn'])
    
    for snstopic in snstopicarn:
        
        value_cost=''
        value_env=''
        value_name=''
        value_createdby = ''
        value_approle = ''
        value_app = ''
        value_comp=''
        value_lob=''
        snsname = ''
        
        response = client.list_tags_for_resource(
            ResourceArn=snstopic
            )

        snsname = snstopic.split(':')[5]
        tag_list = response['Tags']
    
        for i in range(len(tag_list)):
            if tag_list[i]['Key'] == 'Name':
                value_name = tag_list[i]['Value']
            if tag_list[i]['Key'] == 'environment':
                value_env = tag_list[i]['Value']
            if tag_list[i]['Key'] == 'costcenter':
                value_cost = tag_list[i]['Value']
            if tag_list[i]['Key'] == 'createdby':
                value_createdby = tag_list[i]['Value']
            if tag_list[i]['Key'] == 'applicationrole':
                value_approle = tag_list[i]['Value']
            if tag_list[i]['Key'] == 'application':
                value_app = tag_list[i]['Value']
            if tag_list[i]['Key'] == 'compliance':
                value_comp = tag_list[i]['Value']
            if tag_list[i]['Key'] == 'lob':
                value_lob = tag_list[i]['Value']
            
        tag_out = [{"Account Name":account_name,"Account ID":account_id,"SNS Name": snsname,"Name" : value_name, "environment": value_env, "costcenter":value_cost, "createdby":value_createdby,"applicationrole":value_approle,"application":value_app,"compliance":value_comp,"lob":value_lob}]
            
        for i in tag_out:
            ls_out.append(i)

    df = pd.DataFrame(ls_out)
       
    return df
    
def sqs_describe(account_name='controller-account', account_id= sts_client.get_caller_identity()['Account'],rolearn=rolearn_current):
    
    tag_list = []
    tag_out ={}
    ls_out = []
    sqsurl = []
    
    if rolearn != rolearn_current:
        client = assume_role.client_func(rolearn, 'sqs')
    else:
        client = sqs_client
   
    res = client.list_queues (
        )
    if 'QueueUrls' in res.keys():
        for i in res['QueueUrls']:
            sqsurl.append(i)
        
        for queue in sqsurl:
            
            value_cost=''
            value_env=''
            value_name=''
            value_createdby = ''
            value_approle = ''
            value_app=''
            value_comp=''
            value_lob=''
            sqsname = ''
            
            try:
                response = client.list_queue_tags(
                    QueueUrl=queue
                    )
                
                sqsname = queue.split('/')[4]
            
                tag_list = response['Tags']
        
                for i in range(len(tag_list)):
                    if 'Name' in tag_list:
                        value_name = tag_list['Name']
                    if 'environment' in tag_list:
                        value_env = tag_list['environment']
                    if 'costcenter' in tag_list:
                        value_cost = tag_list['costcenter']
                    if 'createdby' in tag_list:
                        value_createdby = tag_list['createdby']
                    if 'applicationrole' in tag_list:
                        value_approle = tag_list['applicationrole']
                    if 'application' in tag_list:
                        value_app = tag_list['application']
                    if 'compliance' in tag_list:
                        value_comp = tag_list['compliance']
                    if 'lob' in tag_list:
                        value_lob = tag_list['lob']
                
                tag_out = [{"Account Name":account_name,"Account ID":account_id,"SQS Name": sqsname,"Name" : value_name, "environment": value_env, "costcenter":value_cost, "createdby":value_createdby,"applicationrole":value_approle,"application":value_app,"compliance":value_comp,"lob":value_lob}]
                
                for i in tag_out:
                    ls_out.append(i)
            
            except:
                tag_out = [{"Account Name": account_name,"Account ID": account_id,"SQS Name": sqsname,"Name" : '', "environment": '', "costcenter": '', "createdby": '',"applicationrole": '',"application": '',"compliance": '',"lob": ''}]
                
                for i in tag_out:
                    ls_out.append(i)
        
        df = pd.DataFrame(ls_out)
        
        return df
    
def events_describe(account_name='controller-account', account_id= sts_client.get_caller_identity()['Account'],rolearn=rolearn_current):
    
    rule_list = []
    tag_out ={}
    ls_out = []
    
    if rolearn != rolearn_current:
        client = assume_role.client_func(rolearn, 'events')
    else:
        client = events_client
    
    res = client.list_rules(
        )
    for i in res['Rules']:
        n = i['Arn']
        rule_list.append(n)
    
    for j in rule_list:
        value_cost=''
        value_env=''
        value_name=''
        value_createdby = ''
        value_approle = ''
        value_app=''
        value_comp=''
        value_lob=''
        
        response = client.list_tags_for_resource(
            ResourceARN=j
            )
        rulename= j.split(':')[5]
        rulename= j.split('/')[1]
    
        try:
            tag_list = response['Tags']
        
            for i in range(len(tag_list)):
                if tag_list[i]['Key'] == 'Name':
                    value_name = tag_list[i]['Value']
                if tag_list[i]['Key'] == 'environment':
                    value_env = tag_list[i]['Value']
                if tag_list[i]['Key'] == 'costcenter':
                    value_cost = tag_list[i]['Value']
                if tag_list[i]['Key'] == 'createdby':
                    value_createdby = tag_list[i]['Value']
                if tag_list[i]['Key'] == 'applicationrole':
                    value_approle = tag_list[i]['Value']
                if tag_list[i]['Key'] == 'application':
                    value_app = tag_list[i]['Value']
                if tag_list[i]['Key'] == 'compliance':
                    value_comp = tag_list[i]['Value']
                if tag_list[i]['Key'] == 'lob':
                    value_lob = tag_list[i]['Value']
            
            tag_out = [{"Account Name":account_name,"Account ID":account_id,"Rule Name": rulename,"Name" : value_name, "environment": value_env, "costcenter":value_cost, "createdby":value_createdby,"applicationrole":value_approle,"application":value_app,"compliance":value_comp,"lob":value_lob}]
            
            for i in tag_out:
                ls_out.append(i)
        
        except:
            tag_out = [{"Account Name":account_name,"Account ID":account_id,"Rule Name": rulename, "Name" : '', "environment": '', "costcenter":'',"createdby":'',"applicationrole":'',"application":'',"compliance":'',"lob":''}]
            
            for i in tag_out:
                ls_out.append(i)
    
    df = pd.DataFrame(ls_out)
    
    return df

def cwlogs_describe(account_name='controller-account', account_id= sts_client.get_caller_identity()['Account'],rolearn=rolearn_current):
    
    loggroups = []
    tag_out ={}
    ls_out = []
    
    
    if rolearn != rolearn_current:
        client = assume_role.client_func(rolearn, 'cwlogs')
    else:
        client = cwlog_client
    
    res = client.describe_log_groups(
        )
    for i in res['logGroups']:
        n = i['logGroupName']
        loggroups.append(n)
    while 'nextToken' in res.keys():
        print('inside loop')
        res = client.describe_log_groups(
                nextToken=res['nextToken'],
                )
    
        for i in res['logGroups']:
            n = i['logGroupName']
            loggroups.append(n)   
        time.sleep(1)
        print('inside loop after sleep')
    
    for j in loggroups:
        tag_name = ""
        tag_env = ""
        value_createdby= ''
        tag_cost1=''
        tag_approle = ''
        value_app=''
        value_comp=''
        value_lob=''
        
        print(j)
        time.sleep(0.2)
        tags = client.list_tags_log_group(
            logGroupName= j
            )
        if 'tags' in tags:
            b =  tags['tags']
            for i in b:
                if i == "Name":
                    tag_name = b['Name']
                if i == "applicationrole":
                    tag_approle = b['applicationrole']
                if i == "costcenter":
                    tag_cost1 = b['costcenter']
                if i == "environment":
                    tag_env = b['environment']
                if i == 'createdby':
                    value_createdby = b['createdby']
                if i == 'application':
                    value_app = b['application']
                if i == 'compliance':
                    value_comp = b['compliance']
                if i == 'lob':
                    value_lob = b['lob']
            
        tag_out = [{"Account Name":account_name,"Account ID":account_id,'LogGroup Name': j, 'Name':tag_name, 'environment':tag_env, "costcenter":tag_cost1, "createdby":value_createdby, "applicationrole":tag_approle,"application":value_app,"compliance":value_comp,"lob":value_lob}]
        for j in tag_out:
            ls_out.append(j)
        
    df = pd.DataFrame(ls_out)
    
    return df

def dynamodb_describe(account_name='controller-account', account_id= sts_client.get_caller_identity()['Account'], rolearn=rolearn_current):
    tables_list = []
    tables_arn = []
    tag_out ={}
    ls_out = []
    
    if rolearn != rolearn_current:
        client = assume_role.client_func(rolearn, 'dynamodb')
    else:
        client = ddb_client
    
    table_arn = 'arn:aws:dynamodb:us-east-1:'+account_id+':table/'
 
    tables_response = client.list_tables()
    for i in tables_response['TableNames']:
        tables_list.append(i)
        arn = table_arn+i
        tables_arn.append(arn)
    
    for table in tables_arn:
        
        value_cost=''
        value_env=''
        value_name=''
        value_createdby = ''
        value_approle = ''
        value_app=''
        value_comp=''
        value_lob=''
        tablename = ''
        
        response = client.list_tags_of_resource(
            ResourceArn=table )
   
        tablename = table.split(':')[5]
        tag_list = response['Tags']
        try:
            for i in range(len(tag_list)):
                if tag_list[i]['Key'] == 'Name':
                    value_name = tag_list[i]['Value']
                if tag_list[i]['Key'] == 'environment':
                    value_env = tag_list[i]['Value']
                if tag_list[i]['Key'] == 'costcenter':
                    value_cost = tag_list[i]['Value']
                if tag_list[i]['Key'] == 'createdby':
                    value_createdby = tag_list[i]['Value']
                if tag_list[i]['Key'] == 'applicationrole':
                    value_approle = tag_list[i]['Value']
                if tag_list[i]['Key'] == 'application':
                    value_app = tag_list[i]['Value']
                if tag_list[i]['Key'] == 'compliance':
                    value_comp = tag_list[i]['Value']
                if tag_list[i]['Key'] == 'lob':
                    value_lob = tag_list[i]['Value']
            
            tag_out = [{"Account Name":account_name,"Account ID":account_id,"Table Name": tablename,"Name" : value_name, "environment": value_env, "costcenter":value_cost, "createdby":value_createdby,"applicationrole":value_approle,"application":value_app,"compliance":value_comp,"lob":value_lob}]
            
            for i in tag_out:
                ls_out.append(i)
        except:
            tag_out = [{"Account Name": account_name,"Account ID": account_id,"Table Name": tablename,"Name" : '', "environment": '', "costcenter": '', "createdby": '',"applicationrole": '',"application": '',"compliance": '',"lob": ''}]
            
            for i in tag_out:
                ls_out.append(i)
    df = pd.DataFrame(ls_out)
      
    return df
    
def cwalarms_describe(account_name='controller-account', account_id= sts_client.get_caller_identity()['Account'],rolearn=rolearn_current):
    
    alarms = []
    tag_out ={}
    ls_out = []
    
    
    if rolearn != rolearn_current:
        client = assume_role.client_func(rolearn, 'cwalarms')
    else:
        client = cwalarm_client
    
    res = client.describe_alarms(
        )
    for i in res['MetricAlarms']:
        n = i['AlarmArn']
        alarms.append(n)
    # print(res)
    while 'NextToken' in res.keys():
        print('inside loop')
        res = client.describe_alarms(
                NextToken=res['NextToken'],
                )
        for i in res['MetricAlarms']:
            n = i['AlarmArn']
            alarms.append(n)   
        # time.sleep(1)
        print('in while loop')
    # print(res)
    
    for j in alarms:
        value_cost=''
        value_env=''
        value_name=''
        value_createdby = ''
        value_approle = ''
        value_app=''
        value_comp=''
        value_lob=''
        
        tags = client.list_tags_for_resource(
            ResourceARN= j
            )

        alarm_name = j.split(':')[6]

        tag_list = tags['Tags']
        
        for i in range(len(tag_list)):
            if tag_list[i]['Key'] == 'Name':
                value_name = tag_list[i]['Value']
            if tag_list[i]['Key'] == 'environment':
                value_env = tag_list[i]['Value']
            if tag_list[i]['Key'] == 'costcenter':
                value_cost = tag_list[i]['Value']
            if tag_list[i]['Key'] == 'createdby':
                value_createdby = tag_list[i]['Value']
            if tag_list[i]['Key'] == 'applicationrole':
                value_approle = tag_list[i]['Value']
            if tag_list[i]['Key'] == 'compliance':
                value_comp = tag_list[i]['Value']
            if tag_list[i]['Key'] == 'application':
                value_app = tag_list[i]['Value']
            if tag_list[i]['Key'] == 'lob':
                value_lob = tag_list[i]['Value']
            
        tag_out = [{"Account Name":account_name,"Account ID":account_id,'Alarm Name': alarm_name, 'Name':value_name, 'environment':value_env, "costcenter":value_cost, "createdby":value_createdby, "applicationrole":value_approle, "application":value_app, "compliance":value_comp, "lob":value_lob}]
        for j in tag_out:
            ls_out.append(j)
        
    df = pd.DataFrame(ls_out)
    
    return df