# Fronts kvrouter only. kvnode replicas are deliberately NOT behind this
# (or any) load balancer: writes and linearizable reads need a direct
# connection to a shard's current leader, and an LB would round-robin
# across replicas, silently breaking that requirement. kvrouter is
# stateless and horizontally scalable, so it's the only thing that belongs
# behind an ALB.

resource "aws_security_group" "alb" {
  name_prefix = "${var.name_prefix}-alb-"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_lb" "kvrouter" {
  name               = "${var.name_prefix}-kvrouter"
  internal           = false
  load_balancer_type = "application"
  security_groups    = [aws_security_group.alb.id]
  subnets            = var.public_subnet_ids
}

resource "aws_lb_target_group" "kvrouter" {
  name        = "${var.name_prefix}-kvrouter-tg"
  port        = var.kvrouter_port
  protocol    = "HTTP" # gRPC over h2c; use protocol_version = "GRPC" once ACM cert + HTTPS listener are added
  vpc_id      = var.vpc_id
  target_type = "ip" # required for awsvpc-networked Fargate tasks

  health_check {
    path                = "/healthz"
    healthy_threshold   = 2
    unhealthy_threshold = 3
    interval            = 15
  }
}

resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.kvrouter.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.kvrouter.arn
  }
}
