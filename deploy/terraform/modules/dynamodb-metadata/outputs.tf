output "table_name" {
  value = aws_dynamodb_table.shard_map.name
}

output "table_arn" {
  value = aws_dynamodb_table.shard_map.arn
}
