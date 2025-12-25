//Create a VPC: Define your VPC resource in Terraform.
resource "aws_vpc" "my_vpc" {
  cidr_block           = var.vpc_cidr_block
  enable_dns_support   = var.vpc_enable_dns_support
  enable_dns_hostnames = var.vpc_enable_dns_hostnames
  tags                 = var.vpc_tags
}

//Create an Internet Gateway: Attach an internet gateway to VPC to enable internet access for resources in the public subnet.
resource "aws_internet_gateway" "my_igw" {
  vpc_id     = aws_vpc.my_vpc.id
  depends_on = [aws_vpc.my_vpc]
}

//Create Public Route Tables
resource "aws_route_table" "public_route_table" {
  vpc_id = aws_vpc.my_vpc.id
  route {
    cidr_block = var.public_route_table_inputs.public_route_table_destination
    gateway_id = aws_internet_gateway.my_igw.id
  }
  tags       = var.public_route_table_inputs.public_route_table_tags
  depends_on = [aws_internet_gateway.my_igw]
}

//Create Public Subnet: Define a public subnet within your VPC.
resource "aws_subnet" "public_subnet" {
  for_each                = var.public_subnet_inputs
  vpc_id                  = aws_vpc.my_vpc.id
  cidr_block              = each.value.public_subnet_cidr_block
  availability_zone       = each.value.public_subnet_az
  map_public_ip_on_launch = each.value.map_public_ip_to_pub_subnet
  tags                    = each.value.public_subnet_tags
  depends_on              = [aws_vpc.my_vpc]
}

resource "aws_route_table_association" "public_subnet_association" {
  for_each       = aws_subnet.public_subnet
  subnet_id      = each.value.id
  route_table_id = aws_route_table.public_route_table.id
  depends_on     = [aws_route_table.public_route_table, aws_subnet.public_subnet]
}

resource "aws_route_table" "private_route_table" {
  vpc_id = aws_vpc.my_vpc.id
  #route {
  #cidr_block = "0.0.0.0/0"
  #}
  tags = var.private_route_table_inputs.private_route_table_tags
  # Add internal routes as needed
}

//Create Private Subnet: Define a private subnet within your VPC.
resource "aws_subnet" "private_subnet" {
  for_each                = var.private_subnet_inputs
  vpc_id                  = aws_vpc.my_vpc.id
  cidr_block              = each.value.private_subnet_cidr_block
  availability_zone       = each.value.private_subnet_az
  map_public_ip_on_launch = each.value.map_public_ip_to_private_subnet
  tags                    = each.value.private_subnet_tags
  depends_on              = [aws_vpc.my_vpc]
}

resource "aws_route_table_association" "private_subnet_association" {
  for_each       = aws_subnet.private_subnet
  subnet_id      = each.value.id
  route_table_id = aws_route_table.private_route_table.id
  depends_on     = [aws_route_table.private_route_table, aws_subnet.private_subnet]
}
