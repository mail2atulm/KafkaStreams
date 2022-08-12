
package kafkastream.basic;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

/**
 * Class to demo basic stream operation like 
 * 1. How to create Kafka stream
 * 2. How to filter message 
 * 3. How to changes message value
 * 4. Save messages in another topic
 * 
 * Following are the high level steps
 * 1. Read the messages from input topic
 * 2. Filer the message if message starts with "example" string
 * 3. Update the message. Let's use apply the uppercase() 
 * 4. Save the message in another output topic.
 * 
 * We can we following command to check the input and output topic messages
 * 
 * ./kafka-console-consumer.sh --bootstrap-server=localhost:9092 --topic InBasicTopic
 * ./kafka-console-consumer.sh --bootstrap-server=localhost:9092 --topic OutBasicTopic
 
 * @author mahajan_a
 *
 */
public class BasicOperations {

  public static void main(String[] args) {
    
    Properties props = Util.getProperties();
    
    //Create the StringBuilder
    StreamsBuilder builder = new StreamsBuilder();

    //Create the KSteram from source topic
    KStream<String, String> sourceStream = builder.stream(Util.sourceTopic,
        Consumed.with(Serdes.String(), Serdes.String()));

    
    sourceStream.filter((kay, value) -> value.startsWith("example")) //Filer the message if message starts with "example" string
      .mapValues((value) -> value.toUpperCase()) // Let's use apply the uppercase() 
      .to(Util.outputTopic, Produced.with(Serdes.String(), Serdes.String())); //Save the message in output topic

    KafkaStreams streams = new KafkaStreams(builder.build(), props);
    
    final CountDownLatch latch = new CountDownLatch(1);
    //Add the shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread("Shutdown") {
      @Override
      public void run() {
        streams.close();
        latch.countDown();
      }
    });

    try {
      streams.start();
      
      //Load some sample message in the source topic
       Thread loadMessageT = new Thread() {
        public void run() {
           new MessageLoader().loadMessages();
        };
      };
      loadMessageT.start();
      
      
      latch.await();
    } catch (Throwable e) {
      System.exit(1);
    }

  }

  

}
