FROM continuumio/miniconda3

RUN conda --version

ARG FUNCTION_DIR="/home/app/"

RUN echo "pull in armadillin"
RUN git clone https://github.com/theosanderson/armadillin.git

WORKDIR /armadillin

RUN conda create --name armadillin python=3.9
RUN echo "conda activate armadillin" >> ~/.bashrc
RUN pip3 install armadillin
RUN pip3 install boto3


WORKDIR ${FUNCTION_DIR}

COPY app.py .
RUN chmod 755 /home/app/app.py
ENTRYPOINT python /home/app/app.py


