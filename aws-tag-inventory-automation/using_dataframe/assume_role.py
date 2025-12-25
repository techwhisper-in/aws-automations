import boto3

stsclient = boto3.client('sts')

def client_func(role_arn, service):
    
    awsaccount = stsclient.assume_role(
        RoleArn = role_arn,
        RoleSessionName = 'Lambda_Session_for_tags')
    
    access_key = awsaccount['Credentials']['AccessKeyId']
    secret_key = awsaccount['Credentials']['SecretAccessKey']
    session_token = awsaccount['Credentials']['SessionToken']
    
    if service == 'dynamodb':
        client = boto3.client('dynamodb',aws_access_key_id=access_key,aws_secret_access_key=secret_key,aws_session_token=session_token)
    if service == 'sns':
        client = boto3.client('sns',aws_access_key_id=access_key,aws_secret_access_key=secret_key,aws_session_token=session_token)
    if service == 'lambda':
        client = boto3.client('lambda',aws_access_key_id=access_key,aws_secret_access_key=secret_key,aws_session_token=session_token)
    if service == 'sqs':
        client = boto3.client('sqs',aws_access_key_id=access_key,aws_secret_access_key=secret_key,aws_session_token=session_token)
    if service == 'events':
        client = boto3.client('events',aws_access_key_id=access_key,aws_secret_access_key=secret_key,aws_session_token=session_token)
    if service == 's3':
        client = boto3.client('s3',aws_access_key_id=access_key,aws_secret_access_key=secret_key,aws_session_token=session_token)
    if service == 'cwlogs':
        client = boto3.client('logs',aws_access_key_id=access_key,aws_secret_access_key=secret_key,aws_session_token=session_token)
    if service == 'cwalarms':
        client = boto3.client('cloudwatch',aws_access_key_id=access_key,aws_secret_access_key=secret_key,aws_session_token=session_token)
    
    return client