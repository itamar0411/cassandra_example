package com.itamar.cassandra.repository;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.itamar.cassandra.entity.Course;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class CourseRepository {

    private Session session;

    private static final String COURSE_TABLE = "university.course";


    public CourseRepository(Session session) {
        this.session = session;
    }

    public void createCourseColumnFamily() {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(COURSE_TABLE).append("(")
                .append("id int,")
                .append("name text,")
                .append("departmentid int,")
                .append("prereq map<int,text>,")
                .append("PRIMARY KEY (departmentid));");

        String query = sb.toString();
        session.execute(query);
    }

    public void insertCourse(Course course) {
        StringBuilder sb = new StringBuilder("INSERT INTO ")
                .append(COURSE_TABLE).append("(id, name, departmentid, prereq) ")
                .append("VALUES (").append(course.getId())
                .append(", '").append(course.getName()+"'")
                .append(", ").append(course.getDepartmentid())
                .append(", ").append(preparePrereq(course.getPrereq())).append(")");
        String query = sb.toString();
        session.execute(query);
    }

    public List<Course> selectAllCourses() {
        String query = "SELECT * FROM " + COURSE_TABLE;
        ResultSet rs = session.execute(query);
        List<Course> courseList = rs.all().stream().map(r->rowToCourse(r)).collect(Collectors.toList());
        return courseList;
    }

    private Course rowToCourse(Row row) {
        Course course = new Course();
        course.setId(row.getInt("id"));
        course.setDepartmentid(row.getInt("departmentid"));
        course.setName(row.getString("name"));
        course.setPrereq(row.getMap("prereq", Integer.class, String.class));

        return course;
    }

    public void dropCourseColumnFamily() {
        String query = "DROP TABLE IF EXISTS " + COURSE_TABLE;
        session.execute(query);
    }

    public void deleteAllCourses() {
        String query = "TRUNCATE " + COURSE_TABLE;
        session.execute(query);
    }

    private String preparePrereq(Map<Integer, String> map) {
        Set<Integer> keys = map.keySet();
        String res = "";

        Iterator<Integer> itr = keys.iterator();
        while(itr.hasNext()) {
            Integer key = itr.next();
            String val = map.get(key);
            res += key + ":" + "'" + val + "'";
            if(itr.hasNext()) {
                res += ", ";
            }
        }
        return "{" + res + "}";
    }

}
