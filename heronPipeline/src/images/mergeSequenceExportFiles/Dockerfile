FROM python:latest AS python-image

RUN python -m pip install numpy
RUN python -m pip install pandas
RUN python -m pip install boto3

COPY . .

ENTRYPOINT [ "python", "app.py" ]