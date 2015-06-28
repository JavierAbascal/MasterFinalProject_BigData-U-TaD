package com.webservice.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class WriteToKafka implements Runnable {

  String fileLocation;
  String topic;
  
  public WriteToKafka(String fileLocation){
    this.fileLocation = fileLocation;
  }

  @Override
  public void run() {
    long tStart = System.nanoTime(); //Thread starts
    ExecutorService executor = Executors.newFixedThreadPool(50);
    List<String> lines       = new ArrayList<String>();
    
    try (BufferedReader br = new BufferedReader(new FileReader(fileLocation))) {
      String line;
      while ((line = br.readLine()) != null) {
        lines.add(line);
      }
      br.close();
    } 
    catch(Exception e) {
      e.getMessage();
    }
    long tReadFile = System.nanoTime();
    
    // Try it without creating a new kafkaProducer each time
    for(int i = 0; i < lines.size(); i++){
      KafkaProducer kafkaProducer = new KafkaProducer(lines.get(i));
      executor.execute(kafkaProducer);
    }
    
    try {
      executor.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    long tSendKafka = System.nanoTime();
    
    System.out.println("Total Time: " + (tSendKafka - tStart)*1.0e-9 + "\n" +
                  "  ReadFile Time: " + (tReadFile -  tStart)*1.0e-9 + "\n" +
                  "  SendKafka Time:  "+(tSendKafka - tReadFile)*1.0e-9);
    
    new File(fileLocation).delete();
    
    return;
  }

}
