package util;

import org.apache.kafka.common.serialization.Serdes.WrapperSerde;

import kafkastream.state.CityAccumulator;
import model.Emp;
import serializer.JsonDeserializer;
import serializer.JsonSerializer;

public class CityAccumulatorSerde extends WrapperSerde<CityAccumulator> {
  
  public CityAccumulatorSerde() {
      super(new JsonSerializer<>(), new JsonDeserializer<>(CityAccumulatorSerde.class));
  }
}