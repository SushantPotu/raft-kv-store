# Private DNS namespace for peer discovery: kvnode instances register
# themselves here at startup (see internal/discovery) instead of relying on
# hardcoded peer IPs, so the cluster can survive task replacement/rescheduling.
resource "aws_service_discovery_private_dns_namespace" "this" {
  name = "${var.name_prefix}.internal"
  vpc  = var.vpc_id
}

# One Cloud Map service per shard, so `kvnode-<shard_id>.raft-kv-store.internal`
# resolves to that shard's replicas. Created per-shard by the caller via
# for_each on shard_ids.
resource "aws_service_discovery_service" "shard" {
  for_each = toset(var.shard_ids)
  name     = each.value

  dns_config {
    namespace_id = aws_service_discovery_private_dns_namespace.this.id
    dns_records {
      ttl  = 10
      type = "A"
    }
    routing_policy = "MULTIVALUE"
  }

  health_check_custom_config {
    failure_threshold = 1
  }
}
