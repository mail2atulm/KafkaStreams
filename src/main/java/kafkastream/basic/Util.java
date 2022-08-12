package kafkastream.basic;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

public class Util {

  public static String sourceTopic = "InBasicTopic";
  public static String outputTopic = "OutBasicTopic";
  
  public static Properties getProperties() {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "basicexample");
    //Note: Change the broker details as per your setup
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    return props;
  }
  
  public static void main(String[] args) {
    String s = "example Test Message280";
    System.err.println("== "+s.startsWith("example"));
  }
}
