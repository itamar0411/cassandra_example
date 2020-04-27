package com.itamar.cassandra.entity;

import java.util.HashMap;
import java.util.Map;

public class Course {

    private int id;
    private String name;
    private int departmentid;
    private Map<Integer, String> prereq = new HashMap<>();

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public int getDepartmentid() {
        return departmentid;
    }

    public Map<Integer, String> getPrereq() {
        return prereq;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setDepartmentid(int departmentid) {
        this.departmentid = departmentid;
    }

    public void setPrereq(Map<Integer, String> prereq) {
        this.prereq = prereq;
    }
}
