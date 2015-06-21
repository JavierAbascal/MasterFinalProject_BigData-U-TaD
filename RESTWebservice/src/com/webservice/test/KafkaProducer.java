package com.webservice.test;

import java.util.Properties;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer implements Runnable {
    private final kafka.javaapi.producer.Producer<Integer, String> producer;
    private final Properties props = new Properties();
    private final String message;
    
    public KafkaProducer(String message)
    {
      props.put("zookeeper.connect", KafkaProperties.zkConnect);
      props.put("metadata.broker.list", KafkaProperties.kafkaServerURL+":"+KafkaProperties.kafkaServerPort);
      props.put("serializer.class", "kafka.serializer.StringEncoder");
      props.put("request.required.acks", "1");
      // Use random partitioner. Don't need the key type. Just set it to Integer.
      // The message is of type String.
      producer = new kafka.javaapi.producer.Producer<Integer, String>(new ProducerConfig(props));
      this.message = message;
    }
    
    @Override
    public void run() {
      producer.send(new KeyedMessage<Integer,String>(KafkaProperties.topic,message));
      System.out.println(message);
      producer.close();
    }
  }