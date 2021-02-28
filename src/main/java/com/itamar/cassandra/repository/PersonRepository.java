package com.itamar.cassandra.repository;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.itamar.cassandra.entity.Book;
import com.itamar.cassandra.entity.Person;

import java.util.List;
import java.util.stream.Collectors;


public class PersonRepository {

    private Session session;

    private static final String PERSON_TABLE = "library.person";


    public PersonRepository(Session session) {
        this.session = session;
    }

    public void createPersonColumnFamily() {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(PERSON_TABLE).append("(")
                .append("id uuid, ")
                .append("firstname text,")
                .append("lastname text,")
                .append("ssn bigint,")
                .append("PRIMARY KEY (lastname, firstname));");

        String query = sb.toString();
        session.execute(query);
    }

    public void insertPerson(Person person) {
        StringBuilder sb = new StringBuilder("INSERT INTO ")
                .append(PERSON_TABLE).append("(id, firstname, lastname, ssn) ")
                .append("VALUES (").append(person.getId())
                .append(", '").append(person.getFirstName()+"'")
                .append(", '").append(person.getLastName()+"'")
                .append(", ").append(person.getSsn()).append(");");
        String query = sb.toString();
        session.execute(query);
    }

    public List<Person> selectAllPeople() {
        String query = "SELECT * FROM " + PERSON_TABLE;
        ResultSet rs = session.execute(query);
        List<Person> personList = rs.all().stream().map(r->rowToPerson(r)).collect(Collectors.toList());
        return personList;
    }

    public List<String> selectDistinctLastNames() {
        String query = "SELECT DISTINCT lastname FROM " + PERSON_TABLE;
        ResultSet rs = session.execute(query);
        List<String> lastnameList = rs.all().stream().map(r->rowToLastname(r)).collect(Collectors.toList());
        return lastnameList;
    }

    private Person rowToPerson(Row row) {
        Person person = new Person();
        person.setId(row.getUUID("id"));
        person.setFirstName(row.getString("firstname"));
        person.setLastName(row.getString("lastname"));
        person.setSsn(row.getLong("ssn"));

        return person;
    }

    private String rowToLastname(Row row) {
        String lastname = row.getString("lastname");
        return lastname;
    }

    public void dropPersonColumnFamily() {
        String query = "DROP TABLE IF EXISTS " + PERSON_TABLE;
        session.execute(query);
    }

    public void deleteAllPeople() {
        String query = "TRUNCATE " + PERSON_TABLE;
        session.execute(query);
    }


}

