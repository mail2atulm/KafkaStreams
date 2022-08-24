package mock;

import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import model.Emp;

public class EmpProducer {
  private static final Gson gson = new GsonBuilder().disableHtmlEscaping().create();
  private static String topic = "emp";
  
  public EmpProducer(String topic) {
    this.topic = topic;
  }
  
  public EmpProducer() {
  }
  
  
  public static void mockEmp() {
    
    Properties kafkaProps = new Properties();
    kafkaProps.put("bootstrap.servers", "localhost:9092");
    kafkaProps.put("key.serializer",
        "org.apache.kafka.common.serialization.StringSerializer");
    kafkaProps.put("value.serializer",
        "org.apache.kafka.common.serialization.StringSerializer");
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaProps);
    for(int i = 0 ; i < 100;i++) {
      Emp emp = new Emp();
      emp.setId(i);
      if(i % 2==0) {
        emp.setCity("Pune");
        emp.setDept("Pune");
      }else if (i % 3==0) {
        emp.setCity("Delhi");
        emp.setDept("Delhi");
      }else {
        emp.setCity("city-"+i);
        emp.setDept("Delhi-"+i);
      }
      
      String json = getJson(emp);
      
      ProducerRecord<String, String> record = new ProducerRecord<>(topic, emp.getCity(), json);
      Future<RecordMetadata> resp = producer.send(record); //TODO handle errors if any
      try {
        Thread.currentThread().sleep(1000);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      //System.out.println("send "+i);
      
    }
   
    
    
  }
  
  private static <T> String getJson(T obj) {
    return gson.toJson(obj);
  }

  
 
}
