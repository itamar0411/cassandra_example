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

import java.util.ArrayList;
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
        List<String> staff = new ArrayList<>();
        staff.add("Amitai Sasson");
        staff.add("David Sasson");
        staff.add("Hamutal Sasson");
        staff.add("Itamar Sasson");
        course.setStaff(staff);

        Course course2 = new Course();
        course2.setId(6);
        course2.setName("Algorithms2");
        course2.setDepartmentid(255);
        Map<Integer, String> map2 = new HashMap<>();
        map2.put(5, "Algorithms1");
        map2.put(7, "DataStructures");
        course2.setPrereq(map2);

        Course course3 = new Course();
        course3.setId(5);
        course3.setName("Algorithms1");
        course3.setDepartmentid(255);
        Map<Integer, String> map3 = new HashMap<>();
        map3.put(7, "DataStructures");
        course3.setPrereq(map3);

        Course course4 = new Course();
        course4.setId(4);
        course4.setName("ComputerScienceIntro");
        course4.setDepartmentid(255);

        Course course5 = new Course();
        course5.setId(7);
        course5.setName("DataStructures");
        course5.setDepartmentid(255);
        Map<Integer, String> map5 = new HashMap<>();
        map5.put(4, "ComputerScienceIntro");
        course5.setPrereq(map5);

        courseRepository.insertCourse(course);
        courseRepository.insertCourse(course2);
        courseRepository.insertCourse(course3);
        courseRepository.insertCourse(course4);
        courseRepository.insertCourse(course5);

        List<Course> courseList = courseRepository.selectAllCourses();

        assertTrue(courseList.size() > 0);
    }

    @Test
    public void testSelectAllCourses() {

        List<Course> courseList = courseRepository.selectAllCourses();

        assertTrue(courseList.size()>0);
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