resource "aws_iam_role" "redshift_iam_role" {
    name                  = "dwhRole"
    assume_role_policy    = jsonencode(
        {
            Statement = [
                {
                    Action    = "sts:AssumeRole"
                    Effect    = "Allow"
                    Principal = {
                        Service = "redshift.amazonaws.com"
                    }
                },
            ]
            Version   = "2012-10-17"
        }
    )
    managed_policy_arns   = [
        "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
    ]
}
