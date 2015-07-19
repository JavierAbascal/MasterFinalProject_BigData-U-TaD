package com.webservice.test;


public interface KafkaProperties
{
  final static String zkConnect = "52.18.14.198:2181";
  final static  String groupId = "group1";
  final static String topic = "kafkaPrueba";
  final static String kafkaServerURL = "52.18.14.198";
  final static int kafkaServerPort = 9092;
  final static int kafkaProducerBufferSize = 64*1024;
  final static int connectionTimeOut = 100000;
  final static int reconnectInterval = 10000;

}