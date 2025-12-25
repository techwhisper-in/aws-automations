//VPC inputs
variable "vpc_cidr_block" {
  type = string
}
variable "vpc_enable_dns_hostnames" {
  type = bool
}
variable "vpc_enable_dns_support" {
  type = bool
}
variable "vpc_tags" {
  type = map(any)
}

//Public Subnet Inputs
variable "public_subnet_inputs" {
  type = map(any)
}

//Private Subnet Inputs
variable "private_subnet_inputs" {
  type = map(any)
}

//Public Route Table Inputs
variable "public_route_table_inputs" {
}

//Private Route Table Inputs
variable "private_route_table_inputs" {
}
