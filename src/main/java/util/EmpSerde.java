package util;

import org.apache.kafka.common.serialization.Serdes.WrapperSerde;

import model.Emp;
import serializer.JsonDeserializer;
import serializer.JsonSerializer;

public class EmpSerde extends WrapperSerde<Emp> {
    
    public EmpSerde() {
        super(new JsonSerializer<>(), new JsonDeserializer<>(Emp.class));
    }
}

