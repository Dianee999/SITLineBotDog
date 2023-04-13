import json
import boto3

sns = boto3.client('sns')

def lambda_handler(event, context):
    # TODO implement
    # 访问传递的数据
    userId = event.get('userId')
    messageText = event.get('messageText')
    
    sns = boto3.client('sns')
    topic_arn = 'arn:aws:sns:us-east-1:736110309646:ConnectStaff' # 替换为您的 SNS 主题 ARN
    message = f'使用者ID:\n{userId}\n\n訊息:\n{messageText}' # 将结果转换为 JSON 格式
    response = sns.publish(
        TopicArn=topic_arn,
        Message=message
    )
    
    return None