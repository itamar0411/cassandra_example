import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.itamar.cassandra.connector.CassandraConnection;
import com.itamar.cassandra.entity.Course;
import com.itamar.cassandra.entity.Person;
import com.itamar.cassandra.repository.CourseRepository;
import com.itamar.cassandra.repository.PersonRepository;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestCourseRepository {

    private CassandraConnection cassandraConnection = null;
    private Session session = null;
    private CourseRepository courseRepository = null;

    @Before
    public void connect() {
        cassandraConnection = new CassandraConnection();
        cassandraConnection.connect("localhost", 9042);
        this.session = cassandraConnection.getSession();
        courseRepository = new CourseRepository(session);
    }

    @Test
    public void testCourseColumnFamilyCreation() {

        courseRepository.createCourseColumnFamily();

        ResultSet result = session.execute(
                "SELECT * FROM " + "university.course;");

        List<String> columnNames =
                result.getColumnDefinitions().asList().stream()
                        .map(cl -> cl.getName())
                        .collect(Collectors.toList());

        assertEquals(columnNames.size(), 4);
        assertTrue(columnNames.contains("id"));
        assertTrue(columnNames.contains("departmentid"));
        assertTrue(columnNames.contains("name"));
        assertTrue(columnNames.contains("prereq"));
    }

    @Test
    public void testInsertCourse() {
        courseRepository.createCourseColumnFamily();

        Course course = new Course();
        course.setId(1);
        course.setName("AI");
        course.setDepartmentid(255);
        Map<Integer, String> map = new HashMap<>();
        map.put(2, "DataScience");
        map.put(3, "Database");
        map.put(4, "ComputerScienceIntro");
        map.put(5, "Algorithms1");
        map.put(6, "Algorithms2");
        map.put(7, "DataStructures");

        course.setPrereq(map);

        courseRepository.insertCourse(course);

        List<Course> courseList = courseRepository.selectAllCourses();

        assertTrue(courseList.size() > 0);

    }

    @Test
    public void testDeleteAllCourses() {

        courseRepository.deleteAllCourses();

        List<Course> courseList = courseRepository.selectAllCourses();

        assertTrue(courseList.size()==0);
    }

    @Test(expected = InvalidQueryException.class)
    public void testDropCorseTable() {

        courseRepository.dropCourseColumnFamily();

        List<Course> courseList = courseRepository.selectAllCourses();
    }

    @After
    public void closeConnection() {
        cassandraConnection.close();
    }

}