package com.webservice.test;

import java.util.List;
import java.util.Properties;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer {
    private final kafka.javaapi.producer.Producer<Integer, String> producer;
    private final Properties props = new Properties();
    
    public KafkaProducer()
    {
      props.put("zookeeper.connect", KafkaProperties.zkConnect);
      props.put("metadata.broker.list", KafkaProperties.kafkaServerURL+":"+KafkaProperties.kafkaServerPort);
      props.put("serializer.class", "kafka.serializer.StringEncoder");
      props.put("request.required.acks", "1");
      // Use random partitioner. Don't need the key type. Just set it to Integer.
      // The message is of type String.
      producer = new kafka.javaapi.producer.Producer<Integer, String>(new ProducerConfig(props));
    }
    
    public void sendMessages(List<String> lines) {
      for(int i = 0; i < lines.size(); i++){
        producer.send(new KeyedMessage<Integer,String>(KafkaProperties.topic, lines.get(i)));
        System.out.println(lines.get(i));
      }
      props.clear();
      producer.close();
    }
  }