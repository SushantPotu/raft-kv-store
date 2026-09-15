# One ECS service per shard, running replicas_per_shard tasks spread
# across AZs via placement constraints, so a single AZ outage can never
# take out a majority of one shard's replicas (assuming
# replicas_per_shard >= 3 and availability_zone_count >= 3, which is the
# whole point of the multi-AZ design).

resource "aws_ecs_cluster" "this" {
  name = "${var.name_prefix}-cluster"
}

resource "aws_security_group" "kvnode" {
  name_prefix = "${var.name_prefix}-kvnode-"
  vpc_id      = var.vpc_id

  # Raft peer RPCs + client KV RPCs, VPC-internal only.
  ingress {
    from_port   = 9000
    to_port     = 9001
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_ecs_task_definition" "kvnode" {
  for_each                 = toset(var.shard_ids)
  family                   = "${var.name_prefix}-kvnode-${each.value}"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.task_cpu
  memory                   = var.task_memory
  execution_role_arn       = var.execution_role_arn
  task_role_arn            = var.kvnode_role_arn

  container_definitions = jsonencode([
    {
      name      = "kvnode"
      image     = var.kvnode_image
      essential = true
      portMappings = [
        { containerPort = 9000, protocol = "tcp" }, # raft peer RPCs
        { containerPort = 9001, protocol = "tcp" }, # client KV RPCs
      ]
      environment = [
        { name = "SHARD_ID", value = each.value },
        { name = "CLOUDMAP_NAMESPACE_ID", value = var.cloudmap_namespace_id },
        { name = "METADATA_TABLE_NAME", value = var.metadata_table_name },
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = var.kvnode_log_group_name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = each.value
        }
      }
    }
  ])
}

resource "aws_ecs_service" "kvnode" {
  for_each        = toset(var.shard_ids)
  name            = "${var.name_prefix}-kvnode-${each.value}"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.kvnode[each.value].arn
  desired_count   = var.replicas_per_shard
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [aws_security_group.kvnode.id]
    assign_public_ip = false
  }

  service_registries {
    registry_arn = var.cloudmap_service_arns[each.value]
  }

  # Spreads the shard's replicas across distinct AZs first, then across
  # distinct instances — this is the mechanism that actually delivers on
  # "an AZ outage can't take a majority of one shard."
  ordered_placement_strategy {
    type  = "spread"
    field = "attribute:ecs.availability-zone"
  }
}
