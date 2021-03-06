import os
import boto3

AWS_REGION_DEFAULT = 'us-east-1'
AWS_S3_BUCKET_DEFAULT = "lithops-numpywren-exp"
AWS_S3_PREFIX_DEFAULT = "npw"
AWS_ROLE_DEFAULT = 'lithops-execution-role'
AWS_INSTANCE_PROFILE_DEFAULT = 'numpywren_exec_instance_profile'
AWS_LAMBDA_FUNCTION_NAME_DEFAULT = 'numpywren_1'
AWS_SQS_QUEUE_DEFAULT = 'numpywren-jobs-1'
MINIO_ENDPOINT=""

s3 = boto3.resource('s3')

def load(config_filename):
    import yaml
    if os.path.exists(config_filename) == False:
        config_filename = '/tmp/.lithops_config'
        s3.Bucket('lithops-numpywren-exp').download_file('.lithops_config',config_filename)
    res = yaml.safe_load(open(config_filename, 'r'))
    return res

def get_default_config_filename():
    """
    First checks .numpywren_config
    then checks NUMPYWREN_CONFIG_FILE environment variable
    then ~/.numpywren_config
    """
    if 'NUMPYWREN_CONFIG_FILE' in os.environ:
        config_filename = os.environ['NUMPYWREN_CONFIG_FILE']
        # FIXME log this

    elif os.path.exists(".numpywren_config"):
        config_filename = os.path.abspath('.numpywren_config')

    else:
        config_filename = get_default_home_filename()
    return config_filename


def get_default_home_filename():
    default_home_filename = os.path.join(os.path.expanduser("~/.numpywren_config"))
    return default_home_filename

def lithops_config():
    if os.path.exists(".lithops_config"):
        config_filename = os.path.abspath('.lithops_config')
    config_data = load(config_filename)
    return config_data

def default():
    """
    First checks .numpywren_config
    then checks NUMPYWREN_CONFIG_FILE environment variable
    then ~/.numpywren_config
    """
    if os.path.exists(".lithops_config"):
    #     config_filename = os.path.abspath('.numpywren_config')
    # else:
        config_filename = os.path.abspath('.lithops_config')
    # config_filename = get_default_config_filename()
    if config_filename is None:
        raise ValueError("could not find configuration file")

    config_data = load(config_filename)
    return config_data


basic_role = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "",
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": "sts:AssumeRole"
        },
        {
            "Sid": "",
            "Effect": "Allow",
            "Principal": {"Service": "ec2.amazonaws.com"},
            "Action": "sts:AssumeRole"
        },
    ]
}

basic_permissions = {
    "Version": "2012-10-17",
    'Statement': [
        {
            'Effect': 'Allow',
            'Action': [
                's3:ListBucket',
                's3:Put*',
                's3:Get*',
                's3:Delete*',
                's3:*MultipartUpload*'
            ],
            'Resource': '*'
        },
        {
            "Effect": "Allow",
            "Action": "sqs:*",
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": [
                "arn:aws:logs:AWS_REGION:AWS_ACCOUNT_ID:log-group:*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": "sts:AssumeRole",
            "Resource": "arn:aws:iam::AWS_ACCOUNT_ID:role/*"
        },
        {
            "Effect": "Allow",
            "Action": "iam:List*",
            "Resource": "arn:aws:iam::AWS_ACCOUNT_ID:role/*"
        },
        {
            "Effect": "Allow",
            "Action": "ec2:Describe*",
            "Resource": "*"
        },
        {
            "Action": "sqs:*",
            "Resource": "*",
            "Effect": "Allow"
        },
        {
            "Action": [
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": [
                "arn:aws:logs:AWS_REGION:AWS_ACCOUNT_ID:log-group:*:*"
            ],
            "Effect": "Allow"
        }
    ]}
