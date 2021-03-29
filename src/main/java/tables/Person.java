package tables;

import java.io.Serializable;

public class Person implements Serializable {
    private static final long serialVersionUID = 0L;

    private int id;
    private int orgId;
    private String name;
    private int salary;

    public Person(int id, int orgId, String name, int salary) {
        this.id = id;
        this.orgId = orgId;
        this.name = name;
        this.salary = salary;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public void setOrgId(int orgId) {
        this.orgId = orgId;
    }

    public int getOrgId() {
        return orgId;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setSalary(int salary) {
        this.salary = salary;
    }

    public int getSalary() {
        return salary;
    }

    @Override
    public String toString() {
        return "[" + "id=" + id + ", orgId=" + orgId + ", name='" + name + '\'' + ", salary=" + salary + ']';
    }
}
