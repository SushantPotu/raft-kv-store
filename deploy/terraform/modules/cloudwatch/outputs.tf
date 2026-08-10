output "kvnode_log_group_name" {
  value = aws_cloudwatch_log_group.kvnode.name
}

output "kvrouter_log_group_name" {
  value = aws_cloudwatch_log_group.kvrouter.name
}

output "dashboard_name" {
  value = aws_cloudwatch_dashboard.main.dashboard_name
}
