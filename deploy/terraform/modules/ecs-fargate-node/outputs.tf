output "cluster_id" {
  value = aws_ecs_cluster.this.id
}

output "kvnode_security_group_id" {
  value = aws_security_group.kvnode.id
}

output "service_names" {
  value = { for k, v in aws_ecs_service.kvnode : k => v.name }
}
