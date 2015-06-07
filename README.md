# MasterFinalProject_BigData-U-TaD
Final Project for the Master's Degree in BigData of the U-TaD University (Madrid, Spain).

## Description:

Technologies implied: Kafka + Spark

### Kafka considerations###

If you run your Kafka server in a Amazon EC2 instance it is important to set in the `config/server.properties` file the next parameter `advertised.host.name=52.16.238.20 // EC2 IP Address`. Otherwise, you won't be able to connect to your Kafka broker