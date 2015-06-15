package com.webservice.test;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

 
@Path("/file")
public class UploadFileService {
 
  @POST
  @Path("/upload")
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  public Response uploadFile(
    @DefaultValue("none") @QueryParam("token") String token,
    @FormDataParam("file") InputStream uploadedInputStream,
    @FormDataParam("file") FormDataContentDisposition fileDetail) {
 
    String uploadedFileLocation = "C:\\work\\Otros\\DatosRouter\\Upload\\" + fileDetail.getFileName();
 
    // save it
    writeToFile(uploadedInputStream, uploadedFileLocation);
 
    String output = "File uploaded to : " + uploadedFileLocation;
 
    return Response.status(200).entity(output).build();
 
  }
 
  // save uploaded file to new location
  private void writeToFile(InputStream uploadedInputStream,
    String uploadedFileLocation) {
 
    try {
      OutputStream out = new FileOutputStream(new File(
          uploadedFileLocation));
      int read = 0;
      byte[] bytes = new byte[1024];
 
      out = new FileOutputStream(new File(uploadedFileLocation));
      while ((read = uploadedInputStream.read(bytes)) != -1) {
        out.write(bytes, 0, read);
      }
      out.flush();
      out.close();
    } catch (IOException e) {
 
      e.printStackTrace();
    }
 
  }
  
  
  public static void main(String[] args) throws InterruptedException{  
    UploadFileService main = new UploadFileService();  
  }
  
 
}