#################################################
# Stage 1 - bundle base image + runtime
#################################################
ARG FUNCTION_DIR="/home/app/"

# Grab a fresh copy of the image and install GCC
FROM python:latest AS python-image
# Install GCC (Alpine uses musl but we compile and link dependencies with GCC)
# RUN apk add --no-cache \
#     libstdc++

#################################################
# Stage 2 - build function and dependencies
#################################################

FROM python-image AS build-image
# Install aws-lambda-cpp build dependencies
# RUN apt-get install \
#     build-base \
#     libtool \
#     autoconf \
#     automake \
#     libexecinfo-dev \
#     make \
#     cmake \
#     libcurl \
#     zlib-dev \
#     git \
#     bzip2-dev \
#     xz-dev
# Include global args in this stage of the build
# ARG FUNCTION_DIR
# ARG RUNTIME_VERSION
# Create function directory
# RUN mkdir -p ${FUNCTION_DIR}

# COPY requirements.txt .
RUN python -m pip install numpy
RUN python -m pip install pandas
RUN python -m pip install boto3
RUN python -m pip install cython
RUN python -m pip install biopython
# RUN python -m pip install pysam
RUN python -m pip install pysam==0.16.0.1
# RUN git clone https://github.com/cov-ert/datafunk.git
# RUN python -m pip install git+https://github.com/cov-ert/datafunk.git
# WORKDIR /datafunk
# RUN pip install .

#################################################
# Install minimap
#################################################
WORKDIR /usr/local
RUN git clone https://github.com/lh3/minimap2
WORKDIR /usr/local/minimap2

RUN pwd
RUN git checkout v2.3
RUN make && chmod 777 minimap2

#################################################
# Install go
#################################################
WORKDIR /usr/local
RUN wget https://golang.org/dl/go1.17.2.linux-amd64.tar.gz
RUN tar -xzf go1.17.2.linux-amd64.tar.gz
ENV PATH="/usr/local/go/bin:${PATH}"
RUN /usr/local/go/bin/go version
RUN go version
#################################################
# Install goFasta
#################################################
WORKDIR /usr/local
RUN go install github.com/cov-ert/gofasta
# RUN git clone https://github.com/cov-ert/gofasta
# WORKDIR /usr/local/gofasta
# RUN go build
RUN gofasta -h

#################################################
# Copy entry function
#################################################
COPY . .

#################################################
# Stage 3 - final runtime image
#################################################

# Grab a fresh copy of the Python image
FROM python-image as run-image
# Copy in the built dependencies
COPY --from=build-image . . 
COPY --from=build-image /usr/local/minimap2 .
COPY --from=build-image /usr/local/gofasta .
COPY --from=build-image /usr/local/go .

ENTRYPOINT [ "python", "app.py" ]


