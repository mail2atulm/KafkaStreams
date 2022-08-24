package kafkastream.state;

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
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import mock.EmpProducer;
import model.Emp;
import util.CityAccumulatorSerde;
import util.EmpSerde;

/**
 * This example demonstrate the stateful events
 * Here we count total number of employee on boarded in City.
 * 
 * 
 * @author mahajan_a
 *
 */
public class StatefulMain {

  public static String EMP_IN = "emp_in_topic1";
  public static String EMP_COUNT_BY_CITY = "emp_count_by_city1";

  public static void main(String[] args) {

    StreamsConfig streamsConfig = new StreamsConfig(getProperties());

    Serde<Emp> empSerde = new EmpSerde();
    Serde<CityAccumulator> citySerde = new CityAccumulatorSerde();
    
    StreamsBuilder streamsBuilder = new StreamsBuilder();

    KStream<String, Emp> empStream = streamsBuilder.stream(EMP_IN, Consumed.with(Serdes.String(), empSerde))
        .mapValues(value -> value.maskSalary());
    
    String cityStateStore = "emp_count_by_city";
    
    
    KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(cityStateStore);
    
    StoreBuilder<KeyValueStore<String, Integer>> storeBuilder =  Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), Serdes.Integer());
    streamsBuilder.addStateStore(storeBuilder);
    
    //Note: If we do not have keys while loading employee data we need to repartition the data to bring all city data in one partition
    //      For this we can use below CityPartitioner and repartition the data using through() api.
    //CityPartitioner cityPartitioner = new CityPartitioner();
    //KStream<String, Emp> onBorardingStream = empStream.through("emp_by_city_temp",Produced.with(Serdes.String(),empSerde,cityPartitioner ));
    
    KStream<String, CityAccumulator> statefulAccumulator = empStream.transformValues(() ->  
                        new EmpByCityTransformer(cityStateStore),cityStateStore);
    
    
    statefulAccumulator.print(Printed.<String, CityAccumulator>toSysOut().withLabel("emp_by_city_label"));
    statefulAccumulator.to(EMP_COUNT_BY_CITY, Produced.with(Serdes.String(), citySerde));

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
    props.put(StreamsConfig.CLIENT_ID_CONFIG, "StateDemoClientId");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "StateDemoClientGId");
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "StateDemoClientId-APP");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
    props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
    return props;
  }
}
