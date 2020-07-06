package com.itamar.cassandra.repository;

import com.datastax.driver.core.*;
import com.itamar.cassandra.entity.Course;

import java.nio.ByteBuffer;
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
                .append("staff list<text>,")
                .append("lecturerphoto blob,")
                .append("PRIMARY KEY (departmentid,name));");

        String query = sb.toString();
        session.execute(query);
        createIndex("id");
    }

    private void createIndex(String column) {
        StringBuilder sb = new StringBuilder("CREATE INDEX IF NOT EXISTS course_id ON ")
                .append(COURSE_TABLE).append(" (" + column + ");");
        String query = sb.toString();
        session.execute(query);

    }

    public void insertCourse2(Course course) {
        PreparedStatement ps = session.prepare("insert into university.course ( id, name, departmentid, prereq, staff, lecturerphoto) values(?,?,?,?,?,?)");
        BoundStatement boundStatement = new BoundStatement(ps);
        session.execute( boundStatement.bind(
                course.getId(),
                course.getName(),
                course.getDepartmentid(),
                course.getPrereq(),
                course.getStaff(),
                course.getLecturerPhoto()));
    }

    public void insertCourse(Course course) {
        String query = prepareInserCourse(course);
        session.execute(query);
    }

    public void insertCourseWithTTL(Course course, int ttl) {
        String query = prepareInserCourse(course);
        query += " USING TTL " + String.valueOf(ttl);
        session.execute(query);
    }

    private String prepareInserCourse(Course course) {
        StringBuilder sb = new StringBuilder("INSERT INTO ")
                .append(COURSE_TABLE).append("(id, name, departmentid, prereq, staff) ")
                .append("VALUES (").append(course.getId())
                .append(", '").append(course.getName()+"'")
                .append(", ").append(course.getDepartmentid())
                .append(", ").append(preparePrereq(course.getPrereq()))
                .append(", ").append(prepareStaff(course.getStaff()))
                .append(")");
        return sb.toString();
    }

    public List<Course> selectAllCourses() {
        String query = "SELECT * FROM " + COURSE_TABLE;
        ResultSet rs = session.execute(query);
        List<Course> courseList = rs.all().stream().map(r->rowToCourse(r)).collect(Collectors.toList());
        return courseList;
    }

    public Course selectCourseById(int id) {
        String query = "SELECT * FROM " + COURSE_TABLE + " WHERE id = " + String.valueOf(id);
        ResultSet rs = session.execute(query);
        List<Course> courseList = rs.all().stream().map(r->rowToCourse(r)).collect(Collectors.toList());
        if(courseList != null && courseList.size() > 0) {
            return courseList.get(0);
        }
        return null;
    }

    private Course rowToCourse(Row row) {
        Course course = new Course();
        course.setId(row.getInt("id"));
        course.setDepartmentid(row.getInt("departmentid"));
        course.setName(row.getString("name"));
        course.setPrereq(row.getMap("prereq", Integer.class, String.class));
        course.setStaff(row.getList("staff", String.class));
        course.setLecturerPhoto(row.getBytes("lecturerphoto"));
        return course;
    }

    public void dropCourseColumnFamily() {
        String query = "DROP TABLE IF EXISTS " + COURSE_TABLE;
        session.execute(query);
    }

    public void dropCourseIndex() {
        String query = "DROP INDEX IF EXISTS course_id";
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

    private String prepareStaff(List<String> staff) {
        StringBuilder sb = new StringBuilder();
        String res = "";
        Iterator<String> itr = staff.iterator();
        while(itr.hasNext()) {
            String member = itr.next();
            sb.append("'" + member + "'");
            if(itr.hasNext()) {
                sb.append(", ");
            }

        }
        res = "[" + sb.toString() + "]";
        return res;
    }

}
