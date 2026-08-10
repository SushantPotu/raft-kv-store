# Backs internal/metaservice: the shard map (which shard owns which key
# range, who its replicas are, who the current leader is). Leader-change
# writes are conditional on term (see internal/metaservice) so a stale
# report from a deposed leader can never clobber a newer one — that
# conditional-write logic lives in application code, this table just needs
# to support conditional writes, which DynamoDB does natively.
resource "aws_dynamodb_table" "shard_map" {
  name         = "${var.name_prefix}-shard-map"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "shard_id"

  attribute {
    name = "shard_id"
    type = "S"
  }

  point_in_time_recovery {
    enabled = true
  }

  tags = {
    Environment = var.environment
  }
}
