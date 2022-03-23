resource "aws_secretsmanager_secret" "udacity_secret" {
  name                    = "udacity_dend_secret"
  recovery_window_in_days = 0
}


resource "aws_secretsmanager_secret_version" "udacity_secret_version" {
  depends_on    = [module.redshift]
  secret_id     = aws_secretsmanager_secret.udacity_secret.id
  secret_string = jsonencode(
    {
      REDSHIFT_ROLE_ARN = aws_iam_role.redshift_iam_role.arn
      REDSHIFT_ENDPOINT = module.redshift.redshift_cluster_endpoint
      REDSHIFT_DATABASE = module.redshift.redshift_cluster_database
      REDSHIFT_USERNAME = module.redshift.redshift_cluster_username
      REDSHIFT_PASSWORD = module.redshift.redshift_cluster_password
    })
}