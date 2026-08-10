output "ecs_execution_role_arn" {
  value = aws_iam_role.ecs_execution.arn
}

output "kvnode_role_arn" {
  value = aws_iam_role.kvnode.arn
}

output "kvrouter_role_arn" {
  value = aws_iam_role.kvrouter.arn
}

output "chaos_role_arn" {
  value = aws_iam_role.chaos.arn
}
