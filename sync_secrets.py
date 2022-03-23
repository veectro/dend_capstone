import boto3
import json
import urllib.parse

AWS_PROFILE = 'udacity'
AWS_REGION = 'us-west-1'
SECRET_NAME = 'udacity_dend_secret'

session = boto3.session.Session(profile_name=AWS_PROFILE)


def get_secret(secret_name, region_name=AWS_REGION) -> dict:
    """
    Get secret from AWS Secrets Manager
    :param secret_name: secret name in AWS Secrets Manager
    :param region_name: region name
    :param aws_profile: aws profile to used (from ~/.aws/credentials)
    :return: dictionary that contains the secret
    """
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    get_secret_value_response = client.get_secret_value(
        SecretId=secret_name
    )
    if 'SecretString' in get_secret_value_response:
        secret = get_secret_value_response['SecretString']
    else:
        secret = get_secret_value_response['SecretBinary']
    return json.loads(secret)


if __name__ == '__main__':
    try:
        secret = get_secret(SECRET_NAME)
        redshift_conn_id = f"postgresql://{secret['REDSHIFT_USERNAME']}:{secret['REDSHIFT_PASSWORD']}" \
                         f"@{secret['REDSHIFT_ENDPOINT'].split(':')[0]}:{secret['REDSHIFT_ENDPOINT'].split(':')[1]}" \
                         f"/{secret['REDSHIFT_DATABASE']}"

        aws_creds = session.get_credentials().get_frozen_credentials()
        # secret must be encoded in url. Convertion is based on https://www.urlencoder.io/python/
        aws_conn_id = f"aws://{aws_creds.access_key}:{urllib.parse.quote(aws_creds.secret_key, safe='')}@"

        with open('.env', 'w') as f:
            f.write(f'AIRFLOW_CONN_REDSHIFT_CONN_ID={redshift_conn_id}\n')
            f.write(f'AIRFLOW_VAR_REDSHIFT_IAM_ARN={secret["REDSHIFT_ROLE_ARN"]}\n')
            f.write(f'AIRFLOW_CONN_AWS_CONN_ID={aws_conn_id}\n')

    except Exception as e:
        print(e)
