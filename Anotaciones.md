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