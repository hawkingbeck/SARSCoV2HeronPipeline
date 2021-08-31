export CDK_NEW_BOOTSTRAP=1

npx cdk bootstrap aws://889562587392/eu-west-1 --profile heron aws://889562587392/eu-west-1

# npx cdk bootstrap aws://889562587392/eu-west-1 --profile heron \
#     --cloudformation-execution-policies arn:aws:iam::aws:policy/AdministratorAccess \
#     aws://889562587392/eu-west-1