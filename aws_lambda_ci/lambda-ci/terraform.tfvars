assume_role_arn = "afghj"
infra_region = "ap-south-1"
lambda_function_name = "LambdaFunctionTestTfJava"
lambda_execution_role = "arn:aws:iam::590183834329:role/fetch-delete-unattached-ebs-role"
memory_size = 128
timeout = 20
layer_arns = ["arn:aws:lambda:ap-south-1:590183834329:layer:layer1:1","arn:aws:lambda:ap-south-1:590183834329:layer:layer2:1"]
kms_key = "arn:aws:kms:ap-south-1:590183834329:key/5c803437-93f8-4312-bdb1-a3c37c316e0b"
tracing_configuration = "Active"
#lambda_bucket = "techwhisper-ami-snapshot-ebs-volume-inventory"
#lambda_package = "change_log_retention.zip"
lambda_function_handler = "example.Hello::handleRequest"
lambda_runtime_environment = "java17"
vpc_id = "/lambda/vpcid"
security_group_id = "security_group1"
subnet_a_id = "/lambda/AppSubnetA"
subnet_b_id = "/lambda/AppSubnetB"
subnet_c_id = "/lambda/AppSubnetC"
lambda_tags = {
    name="lambdaName"
    env="test"
}
lambda_env_variable = {
    env="test"
    createdby="tf"
}