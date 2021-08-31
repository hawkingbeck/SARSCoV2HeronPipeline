
# Log into AWS ECR
aws ecr get-login-password --region eu-west-1 | sudo docker login --username AWS --password-stdin 889562587392.dkr.ecr.eu-west-1.amazonaws.com

# Build the image and push it to AWS ECR
docker build --no-cache --tag 889562587392.dkr.ecr.eu-west-1.amazonaws.com/pangolin:latest .
sudo docker push 889562587392.dkr.ecr.eu-west-1.amazonaws.com/pangolin:latest