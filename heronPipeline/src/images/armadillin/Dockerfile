FROM continuumio/miniconda3

RUN conda --version

ARG FUNCTION_DIR="/home/app/"

RUN apt-get update -y
RUN apt-get install gcc -y

RUN echo "pull in armadillin"
RUN git clone https://github.com/theosanderson/armadillin.git --branch v0.53

WORKDIR /armadillin
RUN conda create --name armadillin python=3.9
RUN echo "conda activate armadillin" >> ~/.bashrc
RUN pip3 install armadillin
RUN pip3 install boto3


WORKDIR ${FUNCTION_DIR}


COPY app.py .
COPY entrypoint.sh .
RUN chmod 755 /home/app/app.py
RUN chmod 755 /home/app/entrypoint.sh
ENTRYPOINT sh /home/app/entrypoint.sh


