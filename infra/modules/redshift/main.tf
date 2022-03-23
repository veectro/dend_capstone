resource "aws_redshift_cluster" "default" {
  cluster_identifier     = var.cluster_identifier
  database_name          = var.db_name
  master_username        = var.username
  master_password        = random_password.password.result
  node_type              = var.node_type
  cluster_type           = var.cluster_type
  number_of_nodes        = var.number_of_nodes
  skip_final_snapshot    = true
  vpc_security_group_ids = [aws_security_group.default.id]
  iam_roles              = var.iam_roles
}

resource "random_password" "password" {
  length      = 16
  special     = false
  min_numeric = 1
  min_upper   = 1
}

resource "aws_security_group" "default" {
  name        = "redshift-default-security-group"
  description = "Security group for redshift, allow all inbound traffic"

  ingress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}