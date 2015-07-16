package com.spark.javaemail;


import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import javax.mail.Transport;
import javax.mail.Authenticator;

public class SendMail {

  /*
   * Normal Conf Parameters for Gennion
    public static final String USERNAME = "alertas@gennion.com";
    public static final String PASSWORD = "CUiDeActOr";
    public static final String HOST     = "smtp.gmail.com";
    public static final String SSL_PORT = "465";
  */

    String username = "";
    String password = "";
    String host     = "";
    String ssl_port = "";

    // Class constructor
    public SendMail(String host, String ssl_port, String username, String password){
        this.host     = host;
        this.ssl_port = ssl_port;
        this.username = username;
        this.password = password;
    }

    //////////////////////////////////////////////////////////////////////////////
    public int sendEmail(
            String from,
            List<String> to,
            String subject,
            String body,
            List<String> attachments)  {
        //////////////////////////////////////////////////////////////////////////////


        Properties props = new Properties();
        // SSL
        props.put("mail.smtp.host",                host);
        props.put("mail.smtp.socketFactory.port",  ssl_port);
        props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
        props.put("mail.smtp.auth",                "true");
        props.put("mail.smtp.port",                ssl_port);

        Session session = Session.getDefaultInstance(props,
                new javax.mail.Authenticator() {
                    protected javax.mail.PasswordAuthentication getPasswordAuthentication() {
                        return new javax.mail.PasswordAuthentication(username, password);
                    }
                }
        );

        try{
            // Create a default MimeMessage object.
            MimeMessage message = new MimeMessage(session);
            // Set From: header field of the header.
            message.setFrom(new InternetAddress(from));
            // Set To: header field of the header.
            for(int i = 0; i < to.size(); i++){
                // Add multiple receiver
                message.addRecipient(Message.RecipientType.TO, new InternetAddress(to.get(i)));
            }
            // Set Subject: header field
            message.setSubject(subject);

            // Create the message part
            BodyPart messageBodyPart = new MimeBodyPart();
            // Fill the message
            messageBodyPart.setHeader("Content-Type", "text/html");
            messageBodyPart.setContent(body, "text/html");
            // Create a multipar message
            Multipart multipart = new MimeMultipart();
            // Set text message part
            multipart.addBodyPart(messageBodyPart);
            // Part two is attachment

            // TODO: Hacer un bucle para cada uno de los archivos adjuntos
            for(int i= 0; i < attachments.size(); i++) {
                String file = attachments.get(i);
                File f = new File(file);
                String filename = f.getName();
                messageBodyPart = new MimeBodyPart();
                DataSource source = new FileDataSource(file);
                messageBodyPart.setDataHandler(new DataHandler(source));
                messageBodyPart.setFileName(filename);
                multipart.addBodyPart(messageBodyPart);
            }

            // Send the complete message parts
            message.setContent(multipart );

            // Send message
            Transport.send(message);
            System.out.println("Sent message successfully....");
            return 1;
        }catch (MessagingException mex) {
            mex.printStackTrace();
            return 0;
        }
    }


    //////////////////////////////////////////////////////////////////////////////
    public static void main(String [] args) {
        //////////////////////////////////////////////////////////////////////////////

        List<String> lFiles = new ArrayList<String>();
        //lFiles.add("C:\\work\\logs\\RetailAnalytics\\Daily\\ETL_201505280926.log");
        //lFiles.add("C:\\work\\logs\\RetailAnalytics\\Daily\\ETL_201505291315.log");

        String host     = "smtp.gmail.com";
        String ssl_port = "465";
        String username = "RaspberryPi2.Sensor.System@gmail.com";
        String password = "lacasadeDJM";

        List<String> to = Arrays.asList("javier.abascal@hotmail.com");

        SendMail sm = new SendMail(host, ssl_port, username, password);
        int res = sm.sendEmail(
                "RaspberryPi2.Sensor.System@gmail.com",         // From
                to,                                             // To      Javier A
                "prueba 2 dest     ",                           // Subject
                "Este es el body",                              // Body
                lFiles                                          // Files to attach
        );
    }
}
