FROM continuumio/miniconda3 AS python-image

# Install all python related modules
ARG FUNCTION_DIR="/home/app/"

RUN conda --version
RUN conda --version

RUN echo "install goFasta"
RUN conda install bioconda::gofasta
RUN python -m pip install numpy
RUN python -m pip install pandas
RUN python -m pip install boto3
RUN python -m pip install cython
# RUN python -m pip install pysam
# RUN python -m pip install pysam==0.16.0.1

#################################################
# Install the minimap
#################################################
WORKDIR /usr/local
RUN git clone https://github.com/lh3/minimap2
WORKDIR /usr/local/minimap2

RUN pwd
RUN git checkout v2.3
RUN make && chmod 777 minimap2

COPY . .

FROM python-image as run-image

COPY --from=build-image . . 
COPY --from=build-image /usr/local/minimap2 .

ENTRYPOINT [ "python", "app.py" ]




