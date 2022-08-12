package kafkastream;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;



public class KStreamExample {
  
  static Properties props = null;
  
  public static void main(String[] args) {
    String sourceTopic = "InTopic";
    String outputTopic = "outTopic";
    init();
    
    StreamsBuilder builder = new StreamsBuilder();
    
    KTable<String, String> ktable = builder.table(sourceTopic,Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("ktable-store1")
        .withKeySerde(Serdes.String())
        .withValueSerde(Serdes.String()))
        .mapValues((value) -> value+"HI");
        
    /*
    KTable<String, String> ktable = builder.table(sourceTopic,Consumed.with(Serdes.String(), Serdes.String()))
        .mapValues((value) -> value.trim())
          .filter((kay,value)-> Long.parseLong(value) > 20);
          */
    ktable.toStream()
    .peek((key, value) -> System.out.println("Outgoing record - key " +key +" value " + value))
    .to(outputTopic);
    
    KafkaStreams streams = new KafkaStreams(builder.build(), props);
    
    final CountDownLatch latch = new CountDownLatch(1);
    Runtime.getRuntime().addShutdownHook(new Thread("Shutdown") {
      @Override
      public void run() {
        streams.close();
        latch.countDown();
      }
    });

    try {
      streams.start();
      latch.await();
    } catch (Throwable e) {
      System.exit(1);
    }
    
        
  }
  private static Properties init() {
    props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "basicexample");
    //Note: Change the broker details as per your setup
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    return props;
  }
  
}
