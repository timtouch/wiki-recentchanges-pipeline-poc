terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ---------------------------------------------------------------------------
# Variables
# ---------------------------------------------------------------------------

variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-2"
}

variable "raw_s3_bucket" {
  description = "S3 bucket for raw JSONL files"
  type        = string
  default     = "wiki-raw-poc"
}

variable "image_tag" {
  description = "ECR image tag to deploy (e.g. latest or a git SHA)"
  type        = string
  default     = "latest"
}

variable "user_agent" {
  description = "User-Agent sent to Wikimedia. Must be descriptive with contact info per their policy."
  type        = string
  default     = "wiki-recentchanges-poc/1.0 (chaosbounder@gmail.com)"
}

variable "raw_retention_days" {
  description = "Days to retain raw JSONL in the landing bucket before expiry. Raw is only needed for replay into Bronze; a few days is ample safety margin."
  type        = number
  default     = 1
}

# ---------------------------------------------------------------------------
# S3 lifecycle: expire raw JSONL after a retention window
#
# The raw landing zone grows ~5-6 GB/day at the full firehose volume. Once
# Auto Loader has ingested a file into the Bronze Delta table, the raw JSONL is
# only useful for replay/reprocessing. Expiring it after raw_retention_days
# keeps storage flat (~35 GB at 7 days) instead of growing unbounded.
#
# NOTE: this targets the raw bucket ONLY. The Delta bucket (wiki-delta-poc) is
# never expired here — that's your actual table storage.
# ---------------------------------------------------------------------------

resource "aws_s3_bucket_lifecycle_configuration" "raw_expiry" {
  bucket = var.raw_s3_bucket

  rule {
    id     = "expire-raw-jsonl"
    status = "Enabled"

    filter {
      prefix = "recentchange/"
    }

    expiration {
      days = var.raw_retention_days
    }
  }
}

# ---------------------------------------------------------------------------
# ECR repository
# ---------------------------------------------------------------------------

resource "aws_ecr_repository" "producer" {
  name                 = "wiki-producer"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

# Lifecycle policy: keep last 5 images to control storage costs
resource "aws_ecr_lifecycle_policy" "producer" {
  repository = aws_ecr_repository.producer.name

  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep last 5 images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 5
      }
      action = { type = "expire" }
    }]
  })
}

# ---------------------------------------------------------------------------
# DynamoDB checkpoint table
# ---------------------------------------------------------------------------

resource "aws_dynamodb_table" "checkpoint" {
  name         = "wiki_producer_checkpoint"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "stream_id"

  attribute {
    name = "stream_id"
    type = "S"
  }

  tags = {
    Project = "wiki-poc"
  }
}

# ---------------------------------------------------------------------------
# IAM role for the ECS task
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "ecs_task_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "producer_task" {
  name               = "wiki-producer-task-role"
  assume_role_policy = data.aws_iam_policy_document.ecs_task_assume.json
}

data "aws_iam_policy_document" "producer_task_policy" {
  # S3: write to raw prefix only
  statement {
    actions   = ["s3:PutObject"]
    resources = ["arn:aws:s3:::${var.raw_s3_bucket}/recentchange/*"]
  }

  # S3: list bucket (needed for Auto Loader directory-listing mode too)
  statement {
    actions   = ["s3:ListBucket"]
    resources = ["arn:aws:s3:::${var.raw_s3_bucket}"]
  }

  # DynamoDB: checkpoint table read/write
  statement {
    actions = [
      "dynamodb:GetItem",
      "dynamodb:PutItem",
    ]
    resources = [aws_dynamodb_table.checkpoint.arn]
  }

  # CloudWatch Logs: allow task to write logs
  statement {
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["${aws_cloudwatch_log_group.producer.arn}:*"]
  }
}

resource "aws_iam_role_policy" "producer_task" {
  name   = "wiki-producer-task-policy"
  role   = aws_iam_role.producer_task.id
  policy = data.aws_iam_policy_document.producer_task_policy.json
}

# Separate execution role — ECS needs this to pull the image and start the task
resource "aws_iam_role" "producer_execution" {
  name               = "wiki-producer-execution-role"
  assume_role_policy = data.aws_iam_policy_document.ecs_task_assume.json
}

resource "aws_iam_role_policy_attachment" "producer_execution" {
  role       = aws_iam_role.producer_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# ---------------------------------------------------------------------------
# CloudWatch log group
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "producer" {
  name              = "/ecs/wiki-producer"
  retention_in_days = 14
}

# ---------------------------------------------------------------------------
# ECS cluster + Fargate service
# ---------------------------------------------------------------------------

resource "aws_ecs_cluster" "wiki" {
  name = "wiki-poc"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

data "aws_caller_identity" "current" {}

resource "aws_ecs_task_definition" "producer" {
  family                   = "wiki-producer"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "256"  # 0.25 vCPU
  memory                   = "512"  # 0.5 GB
  task_role_arn            = aws_iam_role.producer_task.arn
  execution_role_arn       = aws_iam_role.producer_execution.arn

  container_definitions = jsonencode([{
    name  = "producer"
    image = "${aws_ecr_repository.producer.repository_url}:${var.image_tag}"

    environment = [
      { name = "S3_BUCKET",            value = var.raw_s3_bucket },
      { name = "S3_PREFIX",            value = "recentchange" },
      { name = "DYNAMO_TABLE",         value = aws_dynamodb_table.checkpoint.name },
      { name = "AWS_REGION",           value = var.aws_region },
      { name = "FLUSH_INTERVAL_SECS",  value = "60" },
      { name = "FLUSH_SIZE_BYTES",     value = "5242880" },
      { name = "USER_AGENT",           value = var.user_agent },
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.producer.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "producer"
      }
    }

    essential = true
  }])
}

# Use default VPC and subnets for POC simplicity
data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

resource "aws_security_group" "producer" {
  name        = "wiki-producer"
  description = "Outbound-only SG for the wiki SSE producer"
  vpc_id      = data.aws_vpc.default.id

  # Producer only needs outbound HTTPS to reach Wikimedia + AWS APIs
  egress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Project = "wiki-poc"
  }
}

resource "aws_ecs_service" "producer" {
  name            = "wiki-producer"
  cluster         = aws_ecs_cluster.wiki.id
  task_definition = aws_ecs_task_definition.producer.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  # Auto-restart on failure is Fargate's default — desired_count=1 means ECS
  # will always try to keep exactly one task running.

  network_configuration {
    subnets          = data.aws_subnets.default.ids
    security_groups  = [aws_security_group.producer.id]
    assign_public_ip = true  # needed to reach Wikimedia + AWS APIs from default VPC
  }
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "ecr_repository_url" {
  description = "ECR URL — use this in your docker push command"
  value       = aws_ecr_repository.producer.repository_url
}

output "ecs_cluster_name" {
  value = aws_ecs_cluster.wiki.name
}

output "dynamodb_checkpoint_table" {
  value = aws_dynamodb_table.checkpoint.name
}
