package com.webservice.test;

import java.util.Properties;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer {
    private final kafka.javaapi.producer.Producer<Integer, String> producer;
    private final String topic;
    private final Properties props = new Properties();
    
    public KafkaProducer(String topic)
    {
      props.put("serializer.class", "kafka.serializer.StringEncoder");
      props.put("metadata.broker.list", KafkaProperties.kafkaServerURL+":"+KafkaProperties.kafkaServerPort);
      props.put("request.required.acks", "1");
      // Use random partitioner. Don't need the key type. Just set it to Integer.
      // The message is of type String.
      producer = new kafka.javaapi.producer.Producer<Integer, String>(new ProducerConfig(props));
      this.topic = topic;
    }
    
    public void sendToKafka(String message) {
        producer.send(new KeyedMessage<Integer, String>(topic, message));
        System.out.println(message);    
    }
  }