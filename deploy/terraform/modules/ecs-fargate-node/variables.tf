variable "name_prefix" {
  type = string
}

variable "aws_region" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "vpc_cidr" {
  type = string
}

variable "private_subnet_ids" {
  type = list(string)
}

variable "shard_ids" {
  type = list(string)
}

variable "replicas_per_shard" {
  type = number
}

variable "kvnode_image" {
  description = "ECR image URI for kvnode, e.g. <account>.dkr.ecr.<region>.amazonaws.com/raft-kv-store/kvnode:latest"
  type        = string
}

variable "execution_role_arn" {
  type = string
}

variable "kvnode_role_arn" {
  type = string
}

variable "cloudmap_namespace_id" {
  type = string
}

variable "cloudmap_service_arns" {
  description = "Map of shard_id -> Cloud Map service ARN, from the cloudmap module."
  type        = map(string)
}

variable "metadata_table_name" {
  type = string
}

variable "kvnode_log_group_name" {
  type = string
}

variable "task_cpu" {
  type    = number
  default = 512
}

variable "task_memory" {
  type    = number
  default = 1024
}
