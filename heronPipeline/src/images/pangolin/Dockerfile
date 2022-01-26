FROM continuumio/miniconda3

ARG FUNCTION_DIR="/home/app/"

RUN conda --version
RUN conda --version
RUN echo "Updating for PLearn"
RUN git clone https://github.com/cov-lineages/pangolin.git --branch v3.1.17
# RUN git clone https://github.com/cov-lineages/pangolin.git
WORKDIR /pangolin
# RUN git describe
RUN conda env create -f environment.yml
# Activate the new enviornment so that all RUN commands will be using it

RUN echo "conda activate pangolin" >> ~/.bashrc
SHELL ["/bin/bash", "--login", "-c"]
RUN pip install .

# # Install Lambda Runtime Interface Client for Python
RUN python -m pip install boto3

WORKDIR ${FUNCTION_DIR}

# Copy handler function
COPY app/ .
RUN chmod 755 /home/app/entryPoint.sh
RUN chmod 755 /home/app/runPango.py
ENTRYPOINT python /home/app/runPango.py
