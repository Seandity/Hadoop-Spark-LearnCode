package com.leishen.project.hadoop.datajoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by shenlei on 2016/11/19 0019.
 */
public class InfoBean implements Writable {

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }

    public String getSex() {
        return sex;
    }

    public String getDepartment() {
        return department;
    }

    public String getFlag() {
        return flag;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public void setDepartment(String department) {
        this.department = department;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }


    public InfoBean() {

    }

    public void set(String id, String name, int age, String sex, String department, String flag) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.sex = sex;
        this.department = department;
        this.flag = flag;
    }

    private String id;
    private String name;
    private int age;
    private String sex;
    private String department;
    private String flag;

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(id);
        dataOutput.writeUTF(name);
        dataOutput.writeInt(age);
        dataOutput.writeUTF(sex);
        dataOutput.writeUTF(department);
        dataOutput.writeUTF(flag);

    }

    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readUTF();
        this.name = dataInput.readUTF();
        this.age = dataInput.readInt();
        this.sex = dataInput.readUTF();
        this.department = dataInput.readUTF();
        this.flag = dataInput.readUTF();
    }
}
