package kafkastream.state;

import model.Emp;

public class CityAccumulator {
  
  private String cityName;
  private int totalEmpInCity;
 
  
  
  public CityAccumulator(String cityName,int count) {
    super();
    this.cityName = cityName;
  }
  
  

  public String getCityName() {
    return cityName;
  }
  public void setCityName(String cityName) {
    this.cityName = cityName;
  }
  public int getTotalEmpInCity() {
    return totalEmpInCity;
  }
  public void setTotalEmpInCity(int totalEmpInCity) {
    this.totalEmpInCity = totalEmpInCity;
  }
  
  public void addEmpToCity(int prevCount) {
    this.totalEmpInCity += prevCount;
}
  
  @Override
  public String toString() {
    return "CityAccumulator [cityName=" + cityName + ", totalEmpInCity=" + totalEmpInCity + "]";
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    
    if (!(obj instanceof CityAccumulator)) return false;
    
    CityAccumulator o = (CityAccumulator)obj;
    return cityName.equalsIgnoreCase(o.getCityName());
  }
  
  @Override
  public int hashCode() {
   return cityName != null?cityName.hashCode():0;
  }

  public static Builder builder(Emp emp) {
    return new Builder(emp);
  }
  public static final class Builder{
    
    private String cityName;
    private int count;
    
    private Builder(Emp emp) {
      this.cityName = emp.getCity();
      this.count = 1;
    }
    
    public CityAccumulator build() {
      return new CityAccumulator(cityName,count);
    }
  }
  
}
