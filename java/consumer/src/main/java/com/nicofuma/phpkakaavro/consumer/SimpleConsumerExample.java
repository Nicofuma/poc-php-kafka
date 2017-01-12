/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nicofuma.phpkakaavro.consumer;

import io.confluent.kafka.serializers.KafkaAvroDecoder;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import org.apache.avro.generic.GenericRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class SimpleConsumerExample {
    private final ConsumerConnector consumer;
    private final String inputTopic;
    private final String zookeeper;
    private final String groupId;
    private final String url;
    private static SimpleConsumerExample sessionizer;

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Please provide command line arguments: "
                    + "schemaRegistryUrl");
            System.exit(-1);
        }

        // currently hardcoding a lot of parameters, for simplicity
        String zookeeper = "zookeeper:2181";
        String groupId = "SimpleConsumerExample";
        String inputTopic = "page_visits";
        String url = args[0];

        // Typically events are considered to be part of the same session if they are less than 30 minutes apart
        // To make this example show interesting results sooner, we limit the interval to 5 seconds


        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                sessionizer.consumer.shutdown();
            }
        });

        sessionizer = new SimpleConsumerExample(zookeeper, groupId, inputTopic, url);
        sessionizer.run();
    }

    public SimpleConsumerExample(String zookeeper, String groupId, String inputTopic, String url) {
        this.consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                new ConsumerConfig(createConsumerConfig(zookeeper, groupId, url)));
        this.zookeeper = zookeeper;
        this.groupId = groupId;
        this.inputTopic = inputTopic;
        this.url = url;
    }

    private Properties createConsumerConfig(String zookeeper, String groupId, String url) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeper);
        props.put("group.id", groupId);
        props.put("schema.registry.url", url);
        props.put("specific.avro.reader", false);
        props.put("auto.offset.reset", "smallest");

        return props;
    }

    private void run() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();

        // Hard coding single threaded consumer
        topicCountMap.put(inputTopic, 1);

        Properties props = createConsumerConfig(zookeeper, groupId, url);
        VerifiableProperties vProps = new VerifiableProperties(props);

        // Create decoders for key and value
        KafkaAvroDecoder avroDecoder = new KafkaAvroDecoder(vProps);
        StringDecoder stringDecoder = new StringDecoder(new VerifiableProperties());

        KafkaStream stream = consumer.createMessageStreams(topicCountMap, stringDecoder, avroDecoder).get(inputTopic).get(0);
        ConsumerIterator it = stream.iterator();
        System.out.println("Ready to start iterating wih properties: " + props.toString());
        System.out.println("Reading topic:" + inputTopic);

        while (it.hasNext()) {
            MessageAndMetadata messageAndMetadata = it.next();

            // Once we release a new version of the avro deserializer that can return SpecificData, the deep copy will be unnecessary
            GenericRecord genericEvent = (GenericRecord) messageAndMetadata.message();

            System.out.println(genericEvent.toString());
        }
    }
}
