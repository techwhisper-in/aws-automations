output "vpc_id" {
  value=aws_vpc.my_vpc.id
}
output "internet_gateway" {
  value = aws_internet_gateway.my_igw.id
}
output "public_route_table" {
  value = aws_route_table.public_route_table.id
}
output "public_subnets" {
  value = [for subnet in aws_subnet.public_subnet : subnet.id]
}
output "public_route_table_association" {
  value = [for association in aws_route_table_association.public_subnet_association : association.id]
}
output "private_route_table" {
  value = aws_route_table.private_route_table.id
}
output "private_subnets" {
  value = [for subnet in aws_subnet.private_subnet : subnet.id]
}
output "private_route_table_association" {
  value = [for association in aws_route_table_association.private_subnet_association : association.id]
}
