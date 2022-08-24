package kafkastream.state;

import java.util.Objects;

import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import model.Emp;

public class EmpByCityTransformer implements ValueTransformer<Emp, CityAccumulator> {

  private KeyValueStore<String, Integer> stateStore;
  private final String storeName;
  private ProcessorContext context;
  
  
  public EmpByCityTransformer(String storeName) {
    Objects.requireNonNull(storeName,"Store name is null");
    this.storeName = storeName;
  }
  
  
  @Override
  public void init(ProcessorContext context) {
    this.context = context;
    stateStore = (KeyValueStore) this.context.getStateStore(storeName);
    
  }

  @Override
  public CityAccumulator transform(Emp value) {
    CityAccumulator cityAccumulator = CityAccumulator.builder(value).build();
    Integer accumulatedEmpPerCitySoFar = stateStore.get(cityAccumulator.getCityName());

    if (accumulatedEmpPerCitySoFar != null) {
      cityAccumulator.setTotalEmpInCity(accumulatedEmpPerCitySoFar+1);
    }
    stateStore.put(cityAccumulator.getCityName(), cityAccumulator.getTotalEmpInCity());

    return cityAccumulator;

  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
    
  }

}
