import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.utils.UUIDs;
import com.itamar.cassandra.connector.CassandraConnection;
import com.itamar.cassandra.entity.Person;
import com.itamar.cassandra.repository.PersonRepository;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestPersonRepository {

    private CassandraConnection cassandraConnection = null;
    private Session session = null;
    private PersonRepository personRepository = null;

    @Before
    public void connect() {
        cassandraConnection = new CassandraConnection();
        cassandraConnection.connect("localhost", 9042);
        this.session = cassandraConnection.getSession();
        personRepository = new PersonRepository(session);
    }

    @Test
    public void testPersonColumnFamilyCreation() {

        personRepository.createPersonColumnFamily();

        ResultSet result = session.execute(
                "SELECT * FROM "  + "library.person;");

        List<String> columnNames =
                result.getColumnDefinitions().asList().stream()
                        .map(cl -> cl.getName())
                        .collect(Collectors.toList());

        assertEquals(columnNames.size(), 4);
        assertTrue(columnNames.contains("id"));
        assertTrue(columnNames.contains("firstname"));
        assertTrue(columnNames.contains("lastname"));
        assertTrue(columnNames.contains("ssn"));
    }

    @Test
    public void testInsertPeople() {
        personRepository.createPersonColumnFamily();

        Person p1 = new Person();
        p1.setId(UUIDs.timeBased());
        p1.setFirstName("Itamar");
        p1.setLastName("Sasson");
        p1.setSsn(1);

        Person p2 = new Person();
        p2.setId(UUIDs.timeBased());
        p2.setFirstName("Amitai");
        p2.setLastName("Sasson");
        p2.setSsn(2);

        Person p3 = new Person();
        p3.setId(UUIDs.timeBased());
        p3.setFirstName("David");
        p3.setLastName("Sasson");
        p3.setSsn(3);

        Person p4 = new Person();
        p4.setId(UUIDs.timeBased());
        p4.setFirstName("Asaf");
        p4.setLastName("Dagai");
        p4.setSsn(4);

        Person p5 = new Person();
        p5.setId(UUIDs.timeBased());
        p5.setFirstName("Tom");
        p5.setLastName("Grohsman");
        p5.setSsn(5);

        Person p6 = new Person();
        p6.setId(UUIDs.timeBased());
        p6.setFirstName("Hamutal");
        p6.setLastName("Sasson");
        p6.setSsn(6);

        personRepository.insertPerson(p1);
        personRepository.insertPerson(p2);
        personRepository.insertPerson(p3);
        personRepository.insertPerson(p4);
        personRepository.insertPerson(p5);
        personRepository.insertPerson(p6);

        List<Person> personList = personRepository.selectAllPeople();

        assertEquals(personList.size(), 6);
    }

    @Test
    public void testSelectAllPeople() {

        List<Person> people = personRepository.selectAllPeople();

        assertTrue(people.size()>0);
    }

    @Test
    public void testSelectDistinctLastNames() {

        List<String> lastnameList = personRepository.selectDistinctLastNames();

        assertTrue(lastnameList.size()>0);
    }

    @Test
    public void testDeleteAllPeople() {

        personRepository.deleteAllPeople();

        List<Person> people = personRepository.selectAllPeople();

        assertTrue(people.size()==0);
    }

    @Test(expected = InvalidQueryException.class)
    public void testDropPeople() {

        personRepository.dropPersonColumnFamily();

        List<Person> people = personRepository.selectAllPeople();
    }

    @After
    public void closeConnection() {
        cassandraConnection.close();
    }

}
