# Least-privilege roles per component. kvnode and kvrouter get distinct
# roles because their access needs differ (kvnode writes leader-change
# reports + registers with Cloud Map; kvrouter only reads the shard map and
# discovers endpoints) — sharing one broad role would violate least
# privilege for no benefit.

data "aws_iam_policy_document" "ecs_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

# --- ECS task execution role (shared) ---
# This is distinct from the per-component task roles below: the execution
# role is what Fargate itself uses to pull the container image from ECR
# and ship logs to CloudWatch, before the container's own code (running as
# the kvnode/kvrouter task role) ever starts.

resource "aws_iam_role" "ecs_execution" {
  name               = "${var.name_prefix}-ecs-execution-role"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume_role.json
}

resource "aws_iam_role_policy_attachment" "ecs_execution" {
  role       = aws_iam_role.ecs_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# --- kvnode ---

resource "aws_iam_role" "kvnode" {
  name               = "${var.name_prefix}-kvnode-role"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume_role.json
}

data "aws_iam_policy_document" "kvnode" {
  statement {
    sid       = "MetadataTableReadWrite"
    actions   = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:Query"]
    resources = [var.metadata_table_arn]
  }
  statement {
    sid       = "CloudMapRegisterDiscover"
    actions   = ["servicediscovery:RegisterInstance", "servicediscovery:DeregisterInstance", "servicediscovery:DiscoverInstances"]
    resources = ["*"] # Cloud Map instance-level ARNs aren't known until registration; scoped by namespace via condition below.
    condition {
      test     = "StringEquals"
      variable = "servicediscovery:NamespaceId"
      values   = [var.cloudmap_namespace_id]
    }
  }
  statement {
    sid       = "CloudWatchMetricsAndLogs"
    actions   = ["cloudwatch:PutMetricData", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "kvnode" {
  name   = "${var.name_prefix}-kvnode-policy"
  role   = aws_iam_role.kvnode.id
  policy = data.aws_iam_policy_document.kvnode.json
}

# --- kvrouter ---

resource "aws_iam_role" "kvrouter" {
  name               = "${var.name_prefix}-kvrouter-role"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume_role.json
}

data "aws_iam_policy_document" "kvrouter" {
  statement {
    sid       = "MetadataTableReadOnly"
    actions   = ["dynamodb:GetItem", "dynamodb:Query", "dynamodb:Scan"]
    resources = [var.metadata_table_arn]
  }
  statement {
    sid       = "CloudMapDiscoverOnly"
    actions   = ["servicediscovery:DiscoverInstances"]
    resources = ["*"]
  }
  statement {
    sid       = "CloudWatchMetricsAndLogs"
    actions   = ["cloudwatch:PutMetricData", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "kvrouter" {
  name   = "${var.name_prefix}-kvrouter-policy"
  role   = aws_iam_role.kvrouter.id
  policy = data.aws_iam_policy_document.kvrouter.json
}

# --- chaosmonkey (Workstream J) ---
# Scoped to resource tags so chaos testing can never accidentally target
# anything outside this project's own ECS tasks.

resource "aws_iam_role" "chaos" {
  name               = "${var.name_prefix}-chaos-role"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume_role.json
}

data "aws_iam_policy_document" "chaos" {
  statement {
    sid       = "StopOwnEcsTasksOnly"
    actions   = ["ecs:StopTask", "ecs:ListTasks", "ecs:DescribeTasks"]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "aws:ResourceTag/Project"
      values   = [var.name_prefix]
    }
  }
}

resource "aws_iam_role_policy" "chaos" {
  name   = "${var.name_prefix}-chaos-policy"
  role   = aws_iam_role.chaos.id
  policy = data.aws_iam_policy_document.chaos.json
}
