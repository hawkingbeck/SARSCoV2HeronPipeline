#################################################
# Stage 1 - bundle base image + runtime
#################################################
ARG FUNCTION_DIR="/home/app/"
ARG RUNTIME_VERSION="3.7"
ARG DISTRO_VERSION="3.12"

# Grab a fresh copy of the image and install GCC
FROM python:latest AS python-alpine

RUN python -m pip install numpy
RUN python -m pip install pandas
RUN python -m pip install boto3
RUN python -m pip install pyyaml

COPY . .

ENTRYPOINT [ "python", "app.py" ]