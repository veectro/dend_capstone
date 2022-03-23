output "redshift_cluster_endpoint" {
  value = aws_redshift_cluster.default.endpoint
}

output "redshift_cluster_username" {
  value = aws_redshift_cluster.default.master_username
}

output "redshift_cluster_password" {
  value = aws_redshift_cluster.default.master_password
}

output "redshift_cluster_database" {
  value = aws_redshift_cluster.default.database_name
}