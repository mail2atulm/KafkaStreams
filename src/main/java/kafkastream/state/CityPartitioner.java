package kafkastream.state;

import org.apache.kafka.streams.processor.StreamPartitioner;

import model.Emp;

public class CityPartitioner implements StreamPartitioner<String, Emp>{

  @Override
  public Integer partition(String topic, String key, Emp value, int numPartitions) {
    return value.getCity().hashCode() % numPartitions;
  }

}
