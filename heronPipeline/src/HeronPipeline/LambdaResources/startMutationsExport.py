import boto3


def lambda_handler(event, context): 
  print(f"Event: {event}")
  print(f"Context: {context}")

  return "Hello World"