variable "aws_region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name, used as a tag/prefix on all resources."
  type        = string
  default     = "dev"
}

variable "shard_count" {
  description = "Number of Raft shards (independent consensus groups) to provision."
  type        = number
  default     = 2
}

variable "replicas_per_shard" {
  description = "Number of replicas per shard, spread across availability_zones."
  type        = number
  default     = 3
}

variable "availability_zone_count" {
  description = "Number of AZs to spread replicas across."
  type        = number
  default     = 3
}

variable "kvnode_image" {
  description = "ECR image URI for kvnode. No default — must be supplied once an ECR repo exists (Checkpoint 2), e.g. via -var or a dev.auto.tfvars file (gitignored)."
  type        = string
}
