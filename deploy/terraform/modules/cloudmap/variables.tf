variable "name_prefix" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "shard_ids" {
  description = "Shard IDs to create a Cloud Map service for (one per Raft group)."
  type        = list(string)
}
