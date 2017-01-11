FROM python

RUN wget -qO - http://packages.confluent.io/deb/3.0/archive.key | apt-key add - && \
    echo "deb [arch=amd64] http://packages.confluent.io/deb/3.0 stable main" >> /etc/apt/sources.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends librdkafka-dev && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN pip install git+http://github.com/confluentinc/confluent-kafka-python.git#egg=confluent-kafka[avro]
