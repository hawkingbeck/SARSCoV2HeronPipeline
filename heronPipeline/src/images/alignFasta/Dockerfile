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
# RUN python -m pip install pysam
RUN python -m pip install pysam==0.16.0.1
# RUN git clone https://github.com/cov-ert/datafunk.git
RUN python -m pip install git+https://github.com/cov-ert/datafunk.git
# WORKDIR /datafunk
# RUN pip install .

#################################################
# Install the function's dependencies
#################################################
WORKDIR /usr/local
RUN git clone https://github.com/lh3/minimap2
WORKDIR /usr/local/minimap2

RUN pwd
RUN git checkout v2.3
RUN make && chmod 777 minimap2
# ./minimap2
# ENV PATH $PATH:/usr/local/minimap2
# ENV PATH="/usr/local/minimap2:${PATH}"
#################################################


#################################################
# Copy handler function
#################################################
# COPY app/* ${FUNCTION_DIR}
COPY . .

#################################################
# Stage 3 - final runtime image
#################################################

# Grab a fresh copy of the Python image
FROM python-image as run-image
# Include global arg in this stage of the build
# ARG FUNCTION_DIR
# Set working directory to function root directory
# WORKDIR ${FUNCTION_DIR}
# Copy in the built dependencies
COPY --from=build-image . . 
COPY --from=build-image /usr/local/minimap2 .

ENTRYPOINT [ "python", "app.py" ]
