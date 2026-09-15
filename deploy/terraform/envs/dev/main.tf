provider "aws" {
  region = var.aws_region
}

locals {
  name_prefix = "raft-kv-store-${var.environment}"
  shard_ids   = [for i in range(var.shard_count) : "shard-${i + 1}"]
}

module "network" {
  source      = "../../modules/network"
  name_prefix = local.name_prefix
  environment = var.environment
  az_count    = var.availability_zone_count
}

module "dynamodb_metadata" {
  source      = "../../modules/dynamodb-metadata"
  name_prefix = local.name_prefix
  environment = var.environment
}

module "cloudmap" {
  source      = "../../modules/cloudmap"
  name_prefix = local.name_prefix
  vpc_id      = module.network.vpc_id
  shard_ids   = local.shard_ids
}

module "cloudwatch" {
  source      = "../../modules/cloudwatch"
  name_prefix = local.name_prefix
  aws_region  = var.aws_region
}

module "iam" {
  source                 = "../../modules/iam"
  name_prefix            = local.name_prefix
  metadata_table_arn     = module.dynamodb_metadata.table_arn
  cloudmap_namespace_id  = module.cloudmap.namespace_id
}

module "alb" {
  source            = "../../modules/alb"
  name_prefix       = local.name_prefix
  vpc_id            = module.network.vpc_id
  public_subnet_ids = module.network.public_subnet_ids
}

module "ecs_fargate_node" {
  source                 = "../../modules/ecs-fargate-node"
  name_prefix            = local.name_prefix
  aws_region             = var.aws_region
  vpc_id                 = module.network.vpc_id
  vpc_cidr               = module.network.vpc_cidr
  private_subnet_ids     = module.network.private_subnet_ids
  shard_ids              = local.shard_ids
  replicas_per_shard     = var.replicas_per_shard
  kvnode_image           = var.kvnode_image
  execution_role_arn     = module.iam.ecs_execution_role_arn
  kvnode_role_arn        = module.iam.kvnode_role_arn
  cloudmap_namespace_id  = module.cloudmap.namespace_id
  cloudmap_service_arns  = module.cloudmap.service_arns
  metadata_table_name    = module.dynamodb_metadata.table_name
  kvnode_log_group_name  = module.cloudwatch.kvnode_log_group_name
}
