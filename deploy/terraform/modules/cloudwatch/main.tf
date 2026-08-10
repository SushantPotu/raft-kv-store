# Custom metrics published by kvnode (see internal/discovery /
# internal/raft instrumentation): raft_leader_elections_total,
# raft_replication_lag_ms, kv_request_latency_ms. This module just wires up
# the dashboard/alarms consuming them — the application is responsible for
# actually calling cloudwatch:PutMetricData (see iam module for that
# permission).

resource "aws_cloudwatch_log_group" "kvnode" {
  name              = "/ecs/${var.name_prefix}/kvnode"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_group" "kvrouter" {
  name              = "/ecs/${var.name_prefix}/kvrouter"
  retention_in_days = 14
}

resource "aws_cloudwatch_metric_alarm" "leader_elections_spike" {
  alarm_name          = "${var.name_prefix}-leader-elections-spike"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods   = 1
  metric_name         = "raft_leader_elections_total"
  namespace           = var.metric_namespace
  period              = 60
  statistic           = "Sum"
  threshold           = 3
  alarm_description   = "More than 3 leader elections in 60s usually means a flapping node or network partition, not a single clean failover."
  treat_missing_data  = "notBreaching"
}

resource "aws_cloudwatch_metric_alarm" "replication_lag_high" {
  alarm_name          = "${var.name_prefix}-replication-lag-high"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "raft_replication_lag_ms"
  namespace           = var.metric_namespace
  period              = 60
  statistic           = "Average"
  threshold           = var.replication_lag_threshold_ms
  treat_missing_data  = "notBreaching"
}

resource "aws_cloudwatch_dashboard" "main" {
  dashboard_name = "${var.name_prefix}-overview"
  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 12
        height = 6
        properties = {
          title   = "Leader elections / min"
          metrics = [[var.metric_namespace, "raft_leader_elections_total"]]
          period  = 60
          stat    = "Sum"
          region  = var.aws_region
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 0
        width  = 12
        height = 6
        properties = {
          title   = "Replication lag (ms)"
          metrics = [[var.metric_namespace, "raft_replication_lag_ms"]]
          period  = 60
          stat    = "Average"
          region  = var.aws_region
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 6
        width  = 24
        height = 6
        properties = {
          title   = "KV request latency (ms), p50/p99"
          metrics = [[var.metric_namespace, "kv_request_latency_ms", { stat = "p50" }], ["...", { stat = "p99" }]]
          period  = 60
          region  = var.aws_region
        }
      }
    ]
  })
}
