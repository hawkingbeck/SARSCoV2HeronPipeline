FROM continuumio/miniconda3

RUN conda --version

ARG FUNCTION_DIR="/home/app/"

RUN echo "pull in armadillin"
<<<<<<< HEAD
RUN git clone https://github.com/theosanderson/armadillin.git --branch v0.20
=======
RUN git clone https://github.com/theosanderson/armadillin.git --branch v0.21
>>>>>>> cf907aaf7618411ac59cb9ee42c5e0d9e3b51014

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


