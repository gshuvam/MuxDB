provider "aws" {
  region = var.aws_region
}

variable "aws_region" {
  default = "us-east-1"
}

# VPC setup
resource "aws_vpc" "muxdb_vpc" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
}

# Subnets
resource "aws_subnet" "subnet_0" {
  vpc_id            = aws_vpc.muxdb_vpc.id
  cidr_block        = "10.0.1.0/24"
  availability_zone = "us-east-1a"
}

# Security group
resource "aws_security_group" "muxdb_sg" {
  name   = "muxdb-sg"
  vpc_id = aws_vpc.muxdb_vpc.id

  ingress {
    from_port   = 50051
    to_port     = 50051
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# MuxDB Control Node (EC2 instance)
resource "aws_instance" "muxdb_control" {
  ami           = "ami-0c55b159cbfafe1f0" # Amazon Linux 2
  instance_type = "t3.medium"
  subnet_id     = aws_subnet.subnet_0.id

  vpc_security_group_ids = [aws_security_group.muxdb_sg.id]

  tags = {
    Name = "muxdb-control-plane"
  }
}

# RDS instances for Postgres Shards
resource "aws_db_instance" "postgres_shards" {
  count               = 2
  allocated_storage   = 20
  engine              = "postgres"
  engine_version      = "15.4"
  instance_class      = "db.t3.micro"
  username            = "muxuser"
  password            = "muxpassword"
  skip_final_snapshot = true
  db_subnet_group_name = "default"
}

# Elasticache Redis for cache layer
resource "aws_elasticache_cluster" "redis_cache" {
  cluster_id           = "muxdb-redis"
  engine               = "redis"
  node_type            = "cache.t3.micro"
  num_cache_nodes      = 1
  parameter_group_name = "default.redis7"
  port                 = 6379
}
