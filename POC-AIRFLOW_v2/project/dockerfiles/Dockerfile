#FROM apache/airflow:latest
#FROM python:3.8.2-buster
FROM puckel/docker-airflow:latest
#FROM apache/airflow:2.2.1
USER root
# RUN pip install virtualenv
# RUN virtualenv -p python3.7 /env
# # Set virtualenv environment variables. This is equivalent to running# source /env/bin/activate. This ensures the application is executed within# the context of the virtualenv and will have access to its dependencies.\
# ENV VIRTUAL_ENV /env
# ENV PATH /env/bin:$PATH

# RUN mkdir -p /app
# WORKDIR /app
# COPY . /app/

RUN mkdir -p /usr/share/man/man1


#RUN mkdir -p /usr/share/man/man1
#RUN mkdir -p /usr/local/project
RUN mkdir -p /usr/local/airflow/custom
RUN mkdir -p /usr/local/airflow/cloud
#RUN mkdir -p /usr/local/airflow/google
#RUN mkdir -p /usr/local/airflow/azure


#Install OpenJDK-11
RUN apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;

#Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

# Airflow
#ARG AIRFLOW_VERSION=1.10.9
ARG AIRFLOW_USER_HOME=/usr/local/airflow
ARG AIRFLOW_DEPS=""
ARG PYTHON_DEPS=""
ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}
RUN export AIRFLOW_HOME

#WORKDIR /usr/local/airflow
RUN apt update

#RUN apt-get install python3.7.7
RUN apt-get -y install python3-pip
RUN pip install --upgrade pip
RUN python -m pip install --upgrade pip setuptools wheel
#RUN pip install --upgrade google-api-python-client && pip install google-cloud-storage
#RUN pip install apache-airflow-providers-google
RUN pip install importlib

#RUN pip install -r requirements.txt
RUN pip install setuptools>=21.0.0
RUN pip install google-cloud-storage>=1.26.0
RUN pip install pandas
RUN pip install azure-storage-blob==12.8.0
# RUN pip install connexion>=2.0.2
# RUN pip install python_dateutil>=2.6.0
# RUN pip install setuptools>=21.0.0
# RUN pip install python-dotenv>=0.10.3
# RUN pip install sqlalchemy>=0.0.0
# RUN pip install psycopg2>=0.0.0
# RUN pip install Flask==1.1.1
# RUN pip install Flask-Cors==3.0.8
# RUN pip install Flask-Caching==1.9.0
# RUN pip install flask-serialize==1.1.3
# RUN pip install Flask-SQLAlchemy==2.4.1
# RUN pip install python-keycloak>=0.18.0
# RUN pip install google-cloud-pubsub==2.1.0
# RUN pip install google-auth>=1.11.0
# RUN pip install pyjwt>=1.7.1
# RUN pip install python-json-logger>=0.1.11
# RUN pip install fpdf>=1.7.2
# RUN pip install google-cloud-storage>=1.26.0
# RUN pip install pandas==1.0.1
# RUN pip install python-jose>=3.1.0
# RUN pip install shortuuid==1.0.1
# RUN pip install cryptography==3.3
# RUN pip install azure-storage-blob==12.8.0


#RUN cp /usr/local/lib/python3.7/site-packages/pandas/__init__.py /usr/local/lib/python3.7/site-packages/google/
#RUN cp /usr/local/lib/python3.7/site-packages/pandas/__init__.py /usr/local/lib/python3.7/site-packages/azure/
#RUN cp /usr/bin/yes /opt/
#ENV PYTHONPATH=${PYTHONPATH}:/usr/local/lib/python3.7/
ENV PYTHONPATH /usr/local/lib/python3.7/
RUN export PYTHONPATH
#WORKDIR ${AIRFLOW_HOME}
USER airflow

# Airflow
#ARG AIRFLOW_VERSION=1.10.9
#ARG AIRFLOW_USER_HOME=/usr/local/airflow
#ARG AIRFLOW_DEPS=""
#ARG PYTHON_DEPS=""
#ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}





# RUN pip install google-cloud-storage>=1.26.0
# RUN pip install pandas==1.0.1
# RUN pip install azure-storage-blob==12.8.0
# #COPY ./script/entrypoint.sh /entrypoint.sh
# #COPY ./config/airflow.cfg ${AIRFLOW_USER_HOME}/airflow.cfg
# #ENTRYPOINT ["/entrypoint.sh"]
# #CMD ["webserver"]
#
# USER airflow
