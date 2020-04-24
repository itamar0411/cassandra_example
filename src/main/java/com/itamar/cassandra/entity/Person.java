package com.itamar.cassandra.entity;

import java.util.UUID;

public class Person {

    private UUID id;
    private String firstName;
    private String lastName;
    private long ssn;

    public UUID getId() {
        return id;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public long getSsn() {
        return ssn;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public void setSsn(long ssn) {
        this.ssn = ssn;
    }
}
