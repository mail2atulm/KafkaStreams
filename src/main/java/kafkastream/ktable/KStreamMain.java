
package kafkastream.ktable;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * This example demonstrate how to create KTable.
 * We used the materialized view to set the updated values of each key and
 * saving the result in output topic 
 * @author mahajan_a
 *
 */
public class KStreamMain {

  public static void main(String[] args) {
    Properties props = Util.getProperties();

    StreamsBuilder builder = new StreamsBuilder();

    KTable<String, String> ktable = builder
        .table(Util.sourceTopic, Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("ktable-store2")
            .withKeySerde(Serdes.String())
            .withValueSerde(Serdes.String()));

    ktable.toStream()
        .peek((key, value) -> System.out.println("record - key =" + key + "and value = " + value))
        .to(Util.outputTopic);

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
