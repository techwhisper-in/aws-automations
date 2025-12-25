data "aws_ssm_parameter" "vpc_id" {
  name = var.vpc_id
}
data "aws_ssm_parameter" "subnet_a_id" {
  name = var.subnet_a_id
}
data "aws_ssm_parameter" "subnet_b_id" {
  name = var.subnet_b_id
}
data "aws_ssm_parameter" "subnet_c_id" {
  name = var.subnet_c_id
}
data "aws_ssm_parameter" "security_group_id" {
  name = var.security_group_id
}
output "name-sg" {
  value = data.aws_ssm_parameter.subnet_a_id
  sensitive = true
}