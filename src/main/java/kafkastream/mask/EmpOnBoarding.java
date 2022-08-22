
package kafkastream.mask;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import mock.EmpProducer;
import model.Emp;
import util.EmpSerde;

/**
 * 
 * @author mahajan_a
 * 1. Get the employee onboarding message
 * 2. Mask the employee salary before saving emp data in output topic
 * 3. Use following command to open command line consumer
 *    Terminal 1 = ./kafka-console-consumer.sh --bootstrap-server=localhost:9092 --topic emp
 *    Terminal 2= ./kafka-console-consumer.sh --bootstrap-server=localhost:9092 --topic empmask
 * 4. Run this class
 */
public class EmpOnBoarding {

  public static String EMP_IN = "emp";
  public static String EMP_MASK_SALARY = "empmask";

  public static void main(String[] args) {

    StreamsConfig streamsConfig = new StreamsConfig(getProperties());

    Serde<Emp> empSerde = new EmpSerde();

    StreamsBuilder streamsBuilder = new StreamsBuilder();

    KStream<String, Emp> empStream = streamsBuilder.stream(EMP_IN, Consumed.with(Serdes.String(), empSerde))
        .mapValues(value -> value.maskSalary());

    empStream.to(EMP_MASK_SALARY, Produced.with(Serdes.String(), empSerde));

    KafkaStreams stream = new KafkaStreams(streamsBuilder.build(), streamsConfig);

    final CountDownLatch latch = new CountDownLatch(1);
    //Add the shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread("Shutdown") {
      @Override
      public void run() {
        stream.close();
        latch.countDown();
      }
    });
    try {
      stream.start();

      //Load some sample message in the source topic
      Thread loadMessageT = new Thread() {
        public void run() {
          new EmpProducer(EMP_IN).mockEmp();
        };
      };
      loadMessageT.start();

      latch.await();
    } catch (Throwable e) {
      System.exit(1);
    }
  }

  private static Properties getProperties() {
    Properties props = new Properties();
    props.put(StreamsConfig.CLIENT_ID_CONFIG, "EmpOnBoaringCID");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "EmpOnBoaringGID");
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "EmpOnBoaring-APP");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
    props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
    return props;
  }
}
