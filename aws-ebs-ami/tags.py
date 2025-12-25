import boto3
import csv
li=['AccountId','Resource','Service','Region']
l2=[]
def lambda_handler(event,context):
    for reg in ['ap-south-1','us-east-1','ap-northeast-2']:
        get_tags_all(reg)
    #get_tags_s3()
    #csv_writer()
    '''try:
        for reg in ['ap-south-1','us-east-1','ap-northeast-2']:
            get_tags_all(reg)
        get_tags_s3()
        csv_writer()
        #notification()
    except:
        error_notification()'''

def taglist(resourse,item,Service,Region):
    d={'AccountId':494829558485,'Resource':resourse,'Service':Service,'Region':Region}
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
    #print(resourse,"  ",d)
    l2.append(d)
    
def get_tags_all(reg):
    '''ec2 = boto3.client('ec2',region_name=reg)
    paginator = ec2.get_paginator('describe_instances')
    for page in paginator.paginate():
        for j in range(len(page['Reservations'])):
            instances=page['Reservations'][j]['Instances']
            print(j)
            for i in instances:
                instanceid=i['InstanceId']
                try:
                    tag=i['Tags']
                except:
                    tag={}
            print(instanceid,tag,' EC2 Instance ',reg)
            
            taglist(instanceid,tag,'EC2 Instance',reg)


    paginator = ec2.get_paginator('describe_volumes')
    for page in paginator.paginate():
        #print(page)
        #print("_______________")
        for j in range(len(page['Volumes'])):
            volid=page['Volumes'][j]['VolumeId']
            try:
                tag=page['Volumes'][j]['Tags']
            except:
                tag={}
            taglist(volid,tag,'Volumes',reg)
                


    sns=boto3.resource('sns',region_name=reg)
    sn=boto3.client('sns',region_name=reg)
    for topic in sns.topics.all():
        try:
            t=sn.list_tags_for_resource(ResourceArn=topic.arn)
            #print(topic.arn.split(":")[5]," = ",taglist(t['Tags']))
            taglist(topic.arn.split(":")[5],t['Tags'],'SNS',reg)
        except:
            #print(topic.arn.split(":")[5],' = ',taglist({}))
            taglist(topic.arn.split(":")[5],{},'SNS',reg)'''
    sn=boto3.client('sns',region_name=reg)
    paginator=sn.get_paginator(list_topics)
    print(paginator)


    '''log=boto3.client('logs',region_name=reg)
    paginator = log.get_paginator('describe_log_groups')
    for page in paginator.paginate():
        #print(page)
        for j in range(len(page['logGroups'])):
            y=str(page['logGroups'][j]['logGroupName'])
            ar=str(page['logGroups'][j]['arn'])
            response=log.list_tags_log_group(logGroupName=y)
            #print(y," = ",response['tags'])
            taglist(y,response['tags'],'Cloudwatch Log Group',reg)


    alarm=boto3.client('cloudwatch',region_name=reg)
    paginator = alarm.get_paginator('describe_alarms')
    for page in paginator.paginate():
        #print(page)
        for j in range(len(page['MetricAlarms'])):
            y=str(page['MetricAlarms'][j]['AlarmName'])
            arn=str(page['MetricAlarms'][j]['AlarmArn'])
            response=alarm.list_tags_for_resource(ResourceARN=arn)
            #print(y," = ",taglist(response['Tags']))
            taglist(y,response['Tags'],'Cloudwatch Alarm',reg)


    event = boto3.client('events',region_name=reg)
    paginator = event.get_paginator('list_rules')
    for page in paginator.paginate():
        #print(page)
        for j in range(len(page['Rules'])):
            y=str(page['Rules'][j]['Name'])
            arn=str(page['Rules'][j]['Arn'])
            response = event.list_tags_for_resource(ResourceARN=arn)
            #print(y," = ",taglist(response['Tags']))
            taglist(y,response['Tags'],'Cloudwatch Event',reg)


    function = boto3.client('lambda',region_name=reg)
    paginator = function.get_paginator('list_functions')
    for page in paginator.paginate():
        #print(page)
        for j in range(len(page['Functions'])):
            y=str(page['Functions'][j]['FunctionName'])
            arn=str(page['Functions'][j]['FunctionArn'])
            response = function.list_tags(Resource=arn)
            #print(y," = ",response['Tags'])
            taglist(y,response['Tags'],'Lambda Function',reg)
            
    sqs = boto3.resource('sqs',region_name=reg)
    sq = boto3.client('sqs')
    for i in sqs.queues.all():
        url=i.url
        y=url.split('/')[4]
        try:
            response = sq.list_queue_tags(QueueUrl=url)
            taglist(y,response['Tags'],'SQS',reg)
        except:
            taglist(y,{},'SQS',reg)
            
    dynamodb = boto3.client('dynamodb',region_name=reg)
    paginator = dynamodb.get_paginator('list_tables')
    for page in paginator.paginate():
        #print(page)
        for y in page['TableNames']:
            response = dynamodb.describe_table(TableName=y)
            arn=response['Table']['TableArn']
            tag = dynamodb.list_tags_of_resource(ResourceArn=arn)
            try:
                taglist(y,tag['Tags'],'DynamoDB_Table',reg)
            except:
                taglist(y,{},'DynamoDB_Table',reg)'''
    
def get_tags_s3():

    s3 = boto3.resource('s3')
    s=boto3.client('s3')
    for bucket in s3.buckets.all():
        #print(bucket)
        reg=s.get_bucket_location(Bucket=bucket.name)['LocationConstraint']
        if reg==None:
            reg='Global'
        else:
            reg=reg
        try:
            t=s.get_bucket_tagging(Bucket=bucket.name)
            #print(bucket.name," = ",taglist(t['TagSet']))
            taglist(bucket.name,t['TagSet'],'S3',reg)
        except:
            #print(bucket.name,' = ',taglist({}))
            taglist(bucket.name,{},'S3',reg)

def csv_writer():
    fieldnames=li
    rows=l2
    s3=boto3.client('s3')
    with open('tag.csv', 'w', encoding='UTF8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    with open('tag.csv','r') as data:
        with open('tags.csv','w',encoding='UTF-8',newline='') as f:
            for line in csv.reader(data):
                for i in range(len(line)):
                    if line[i]=='':
                        line[i]='Not Tagged'
                #print(line)
                writer=csv.writer(f)
                writer.writerows([line])
    #s3.upload_file("tags.csv","tag-csv",'tags.csv')
    
    
def notification():
    sns=boto3.client('sns')
    sns.publish(TopicArn="arn:aws:sns:us-east-1:494829558485:Default_CloudWatch_Alarms_Topic",
    Subject="Fetched tags for account 494829558485",
    Message="Tags have been fetched.\n\nPlease check S3 bucket :- tags-csv\nCSV File Name :- tags.csv")
    
    
def error_notification():
    sns=boto3.client('sns')
    sns.publish(TopicArn="arn:aws:sns:us-east-1:494829558485:Default_CloudWatch_Alarms_Topic",
    Subject="Error occured while fetching tags for account 494829558485",
    Message="Tags couldn't be fetched.\n\nPlease check Cloudwatch Logs")

lambda_handler(str,str)
    