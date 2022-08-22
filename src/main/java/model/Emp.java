package model;

public class Emp {
  
  private int id;
  private String name;
  private String dept;
  private String city;
  private String salary;
  public int getId() {
    return id;
  }
  public void setId(int id) {
    this.id = id;
  }
  public String getName() {
    return name;
  }
  public void setName(String name) {
    this.name = name;
  }
  public String getDept() {
    return dept;
  }
  public void setDept(String dept) {
    this.dept = dept;
  }
  public String getCity() {
    return city;
  }
  public void setCity(String city) {
    this.city = city;
  }
  public String getSalary() {
    return salary;
  }
  public void setSalary(String salary) {
    this.salary = salary;
  }
  @Override
  public String toString() {
    return "Emp [id=" + id + ", name=" + name + ", dept=" + dept + ", city=" + city + ", salary=" + salary + "]";
  }
  
  public Emp maskSalary() {
    setSalary("XXXX");
    return this;
  }
  
  
}
