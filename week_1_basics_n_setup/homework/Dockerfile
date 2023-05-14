FROM python:3.9

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2 pyarrow fastparquet

WORKDIR /app
COPY ingest_data_homwork.py ingest_data_homwork.py

#ENTRYPOINT [ "bash" ]

ENTRYPOINT [ "python","ingest_data_homwork.py" ]

 