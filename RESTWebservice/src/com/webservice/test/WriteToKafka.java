package com.webservice.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

public class WriteToKafka implements Runnable {

  String fileLocation;
  String topic;
  String lineRegex = "\\d{6};[0-9A-F]{2}:[0-9A-F]{2}:[0-9A-F]{2}:[0-9A-F]{2}:[0-9A-F]{2}:[0-9A-F]{2};201[5-9]-[0-1][0-9]-[0-3][0-9];[0-2][0-9]:[0-5][0-9]:[0-5][0-9].\\d{6};-\\d{2}dB;.*;\".*\"";
  
  public WriteToKafka(String fileLocation){
    this.fileLocation = fileLocation;
  }

  @Override
  public void run() {
    long tStart = System.nanoTime(); //Thread starts
    List<String> lines = new ArrayList<String>();
    
    try {
      GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(fileLocation));
      BufferedReader br = new BufferedReader(new InputStreamReader(gzip));
      String line;
      while ((line = br.readLine()) != null) {
        if(line.matches(lineRegex)) {
          lines.add(line);
        }
        else {
          System.out.println("line Rejected: " + line);
        }
      }
      br.close();
      
    } catch (IOException e1) {
      System.out.println("ERROR READING GZIP FILE");
      e1.printStackTrace();
      e1.getMessage();
    }
    
    long tReadFile = System.nanoTime();
    
    KafkaProducer kafkaProducer = new KafkaProducer();
    kafkaProducer.sendMessages(lines);

    long tSendKafka = System.nanoTime();
    
    System.out.println("Total Time: " + (tSendKafka - tStart)*1.0e-9 + "\n" +
                  "  ReadFile Time: " + (tReadFile -  tStart)*1.0e-9 + "\n" +
                  "  SendKafka Time:  "+(tSendKafka - tReadFile)*1.0e-9);
    
    lines.clear();
    new File(fileLocation).delete();
    
    return;
  }

}
