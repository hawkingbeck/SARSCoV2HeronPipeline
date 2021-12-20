#################################################
# Stage 1 - bundle base image + runtime
#################################################
ARG FUNCTION_DIR="/home/app/"
ARG RUNTIME_VERSION="3.7"
ARG DISTRO_VERSION="3.12"

# Grab a fresh copy of the image and install GCC
FROM python:${RUNTIME_VERSION}-alpine${DISTRO_VERSION} AS python-alpine
# Install GCC (Alpine uses musl but we compile and link dependencies with GCC)
RUN apk add --no-cache \
    libstdc++

#################################################
# Stage 2 - build function and dependencies
#################################################

FROM python-alpine AS build-image
# Install aws-lambda-cpp build dependencies
RUN apk add --no-cache \
    build-base \
    libtool \
    autoconf \
    automake \
    libexecinfo-dev \
    make \
    cmake \
    libcurl \
    zlib-dev \
    git \
    bzip2-dev \
    xz-dev
# Include global args in this stage of the build
ARG FUNCTION_DIR
ARG RUNTIME_VERSION
# Create function directory
RUN mkdir -p ${FUNCTION_DIR}

COPY requirements.txt .

# Install Lambda Runtime Interface Client for Python
RUN python${RUNTIME_VERSION} -m pip install awslambdaric --target ${FUNCTION_DIR}

RUN python${RUNTIME_VERSION} -m pip install -r requirements.txt --target ${FUNCTION_DIR}

#################################################
# Copy handler function
# COPY app/* ${FUNCTION_DIR}
COPY app.py ${FUNCTION_DIR}
#################################################

#################################################
# Stage 3 - final runtime image
#################################################

# Grab a fresh copy of the Python image
FROM python-alpine
# Include global arg in this stage of the build
ARG FUNCTION_DIR
# Set working directory to function root directory
WORKDIR ${FUNCTION_DIR}
# Copy in the built dependencies
COPY --from=build-image ${FUNCTION_DIR} ${FUNCTION_DIR}

#### Setup binaries that don't have pip packages
ADD https://github.com/cov-ert/gofasta/releases/download/v0.0.5/gofasta-linux-amd64  /usr/local/bin/gofasta
RUN chmod 755 /usr/local/bin/gofasta

#################################################
# (Optional) Add Lambda Runtime Interface Emulator and use a script in the ENTRYPOINT for simpler local runs
#################################################
# ADD https://github.com/aws/aws-lambda-runtime-interface-emulator/releases/latest/download/aws-lambda-rie /usr/bin/aws-lambda-rie
# COPY entry.sh /
# RUN chmod 755 /usr/bin/aws-lambda-rie /entry.sh
# ENTRYPOINT [ "/entry.sh" ]
# CMD [ "app.handler" ]


# ADD https://github.com/aws/aws-lambda-runtime-interface-emulator/releases/latest/download/aws-lambda-rie /usr/local/bin/aws-lambda-rie
# COPY ./entry_script.sh /entry_script.sh
# RUN chmod 755 /usr/local/bin/aws-lambda-rie /entry_script.sh
# ENTRYPOINT [ "/entry_script.sh" ]

# Stage 4: Entry point
# ENTRYPOINT ["python3", "mutations_import.py" ]

ENTRYPOINT [ "python3", "app.py" ]