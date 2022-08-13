
package kafkastream.ktable;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

/**
 * 
 * @author mahajan_a
 *
 */
public class MessageLoader {
  private AdminClient adminClient = null;

  public static void main(String[] args) {
    new MessageLoader().loadMessages();
    
  }
  public void loadMessages() {
    try {
      Properties props = new Properties();
      props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
      adminClient = AdminClient.create(props);

      createTopic(Util.sourceTopic);
      createTopic(Util.outputTopic);

      for (int i = 0; i <100; i++) {
        sendMessage(i);
        Thread.currentThread().sleep(1000);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  private void createTopic(String topicName) {
    System.out.println("creating topic");
    try {
      short replicationFactor = 1;
      int partitionCount = 4;
      NewTopic topic = new NewTopic(topicName, partitionCount, replicationFactor);
      CreateTopicsOptions o = new CreateTopicsOptions();
      
      CreateTopicsResult rs = adminClient.createTopics(Collections.singleton(topic));

      KafkaFuture<Integer> partition = rs.numPartitions(topicName);
      System.out.println("Topic created with partition =" + partition.get());
    } catch (TopicExistsException e) {
      System.out.println("Topic with name "+topicName+" already present");
    }catch (Exception e) {
      //e.printStackTrace();
    }

  }

  private void ListTopic(AdminClient admin) {
    System.out.println("Listing topics");
    try {
      ListTopicsResult topics = admin.listTopics();
      topics.names().get().forEach(System.out::println);
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  private void sendMessage(int i) throws Exception {
    // System.out.println("Sending Message");

    Properties kafkaProps = new Properties();
    kafkaProps.put("bootstrap.servers", "localhost:9092");
    kafkaProps.put("key.serializer",
        StringSerializer.class.getName());
    kafkaProps.put("value.serializer",JsonSerializer.class.getName());
   // kafkaProps.put("value.serializer",StringSerializer.class.getName());

    KafkaProducer<String, CityInfo> producer = new KafkaProducer<String, CityInfo>(kafkaProps);

    String msgKey = "City1"; 
    CityInfo info = new CityInfo(i, i);
    
    if (i % 2 == 0) {
      msgKey = "Pune";
    }
    
    ProducerRecord<String, CityInfo> record = new ProducerRecord(Util.sourceTopic, msgKey, info);
    Future<RecordMetadata> resp = producer.send(record);

  }

  /*
  static void readMessages() {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", "am3");
    props.put("key.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("auto.offset.reset","earliest");// latest
    
  KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
  consumer.subscribe(Collections.singletonList(topicName));
  while (true) {
    System.out.println("Polling messages..");
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
    for (ConsumerRecord<String, String> record : records) {
      System.out.printf("topic = %s, partition = %d, offset = %d, " +
          "msssage = %s\n",
          record.topic(), record.partition(), record.offset(),
          record.value());
    }
  }
  }
  */
}

//docker exec -it 7d51aba7f99f /bin/bash
// cd /opt/bitnami/kafka/bin

//Create topic
//kafka-topics.sh --create --topic AM1 --bootstrap-server localhost:9092

//Write message to topic
//kafka-console-producer.sh --topic AM1 --bootstrap-server localhost:9092

//Read message from topic
//kafka-console-consumer.sh --topic AM1 --from-beginning --bootstrap-server localhost:9092
