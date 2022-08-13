package kafkastream.ktable;

public class CityInfo {
  
  double temp;
  int humidity;
  public CityInfo(double temp, int humidity) {
    super();
    this.temp = temp;
    this.humidity = humidity;
  }
  public double getTemp() {
    return temp;
  }
  public void setTemp(double temp) {
    this.temp = temp;
  }
  public int getHumidity() {
    return humidity;
  }
  public void setHumidity(int humidity) {
    this.humidity = humidity;
  }
  @Override
  public String toString() {
    return "CityInfo [temp=" + temp + ", humidity=" + humidity + "]";
  }
  
  
}
