variable "name_prefix" {
  type = string
}

variable "aws_region" {
  type = string
}

variable "metric_namespace" {
  type    = string
  default = "RaftKVStore"
}

variable "replication_lag_threshold_ms" {
  type    = number
  default = 500
}
