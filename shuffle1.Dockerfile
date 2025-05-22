FROM python:3.8-bullseye

# Installeer Java zonder problemen met GPG keys
RUN apt-get update && \
    apt-get install -y software-properties-common && \
    apt-get install -y openjdk-11-jdk && \
    apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Installeer pyspark
RUN pip install --no-cache-dir pyspark

# Zet werkmap
WORKDIR /app

# Voeg je bestanden toe
COPY countvotes.py .
COPY generated_votes_be.txt .

# Voer script uit en houd container draaiend
CMD python countvotes.py && tail -f /dev/null
