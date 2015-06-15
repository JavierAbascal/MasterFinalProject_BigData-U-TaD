package com.webservice.test;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.webservice.test.KafkaProperties;
import com.webservice.test.KafkaProducer;

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

 
@Path("/file")
public class UploadFileService implements KafkaProperties{
  
  @POST
  @Path("/upload")
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  public Response uploadFile(
    @DefaultValue("none") @QueryParam("token") String token,
    @FormDataParam("file") InputStream uploadedInputStream,
    @FormDataParam("file") FormDataContentDisposition fileDetail) throws FileNotFoundException, IOException {
 
    if (!token.equals("c512623ef8144b3862f19739ccc9fd03"))
      return Response.status(401).entity("Token not valid").build();
    
    String uploadedFileLocation = "C:\\work\\Otros\\DatosRouter\\Upload\\" + fileDetail.getFileName();
 
    // save the file
    boolean resSaveFile = writeToFile(uploadedInputStream, uploadedFileLocation);
    if (resSaveFile == false)
      return Response.status(500).entity("ERROR uploading file").build();
    
    // decompress the file,open the file and send line by line to KafkaServer
    boolean resKafka = writeToKafka(uploadedFileLocation, KafkaProperties.topic);
    if (resKafka == false)
      return Response.status(501).entity("ERROR sending to Kafka-Server").build();
    
    
    
    String output = "File uploaded to : " + uploadedFileLocation;
    return Response.status(200).entity(output).build();

    // delete the file
 
  }
  
  // save uploaded file to new location
  //////////////////////////////////////////////////////////////////////////////
  private boolean writeToFile(InputStream uploadedInputStream,
    String uploadedFileLocation) {
  //////////////////////////////////////////////////////////////////////////////
    try {
      OutputStream out = new FileOutputStream(new File(uploadedFileLocation));
      int read = 0;
      byte[] bytes = new byte[1024];
 
      out = new FileOutputStream(new File(uploadedFileLocation));
      while ((read = uploadedInputStream.read(bytes)) != -1) {
        out.write(bytes, 0, read);
      }
      
      out.flush();
      out.close();
      
      return true;
    } catch (IOException e) {
 
      e.printStackTrace();
      return false;
    }
  }
    
  
  // decompress the file,open the file and send line by line to KafkaServer
  //////////////////////////////////////////////////////////////////////////////
  private boolean writeToKafka(String uploadedFileLocation, String topic) throws FileNotFoundException, IOException{
  //////////////////////////////////////////////////////////////////////////////
    
    KafkaProducer kafkaproducer= new KafkaProducer(topic);
    try (BufferedReader br = new BufferedReader(new FileReader(uploadedFileLocation))) {
      String line;
      while ((line = br.readLine()) != null) {
         kafkaproducer.sendToKafka(line);
      }
      br.close();
    } 
    catch(Exception e)
    {
      e.getMessage();
      return false;
    }
    
    return true;
  }
  
  
  //////////////////////////////////////////////////////////////////////////////
  public static void main(String[] args) throws InterruptedException{
  //////////////////////////////////////////////////////////////////////////////
    UploadFileService main = new UploadFileService();  
  }
  
 
  
  
}