# KAFKA #

- Se necesita Zookeper, investigar como crear un nodo que no sea "quick-and-dirty single-node zookeeper instance"
- Lanzar zookeper
- Lanzar Kafka server


## Cosas: ##

- Abrir puertos en EC2 de zookeper y de kafka
- Particioinamiento de el partitioner de KAFKA
- ACK de kafka para que el envio de datos del producer no sea: "fire and forget"
- Ojito con las instancias EC2 de Amazon. Hay que poner en el archivo server.propierties `advertised.host.name=52.16.238.20`

**A continuación tenemos que entender un poquito mejor como escribir y como leer de Kafka**

# SPARK #

- Spark funciona bien en intellij pero las librerías tardan en descargarse un cojón y medio hahaha
- Bajarse el plugin de Scala y SBT (que es como un maven)
- "Local mode" hay que ponerlo  el código


## WebService GlasFish Java ##

- Es importante exportar las librerías de MAVEN a WEB-INF/lib (en propierties mirar el Deployment Assembler TT)
- Escribir bien el web.xml

Aquí se sube el WAR
/usr/share/tomcat8/webapps/

Aquí se mira los logs 
/usr/share/tomcat8/logs

CircleCI.com para autodespliegue


## Curl instruction ##



curl -F "file=@prueba.txt" http://52.18.1.37:8080/RESTWebservice/rest/file/upload?token=c512623ef8144b3862f19739ccc9fd03

curl -F "file=@prueba.txt" http://localhost:8080/RESTWebservice/rest/file/upload?token=c512623ef8144b3862f19739ccc9fd03

Aquí se sube el WAR
/usr/share/tomcat8/webapps/

Aquí se mira los logs 
/usr/share/tomcat8/logs


## InfluxDB + Grafana##


$ sudo service grafana-server start
$ sudo service influxdb start

o

Iniciar en /etc/init.d/influxdb

### SPARK STREAMNG ###

descargar scala y export al path
descargar spark 

sbin/start-master.sh
sbin/start-slave.sh worker_1 sparkURI

./sbin/start-master.sh -c 1 -m 1G
./sbin/start-slave.sh Worker_1 spark://ip-172-31-37-185:7077 -c 1 -m 1G
./sbin/start-slave.sh Worker_2 spark://ip-172-31-37-185:7077 -c 1 -m 1G
./sbin/start-slave.sh Worker_3 spark://ip-172-31-37-185:7077 -c 1 -m 1G

./bin/spark-submit --class ProbeRequestStreaming --master spark://ip-172-31-37-185:7077 --deploy-mode client /home/ubuntu/spark-test.jar