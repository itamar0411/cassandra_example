package com.itamar.cassandra.repository;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.itamar.cassandra.entity.Book;
import java.util.List;
import java.util.stream.Collectors;

public class BookRepository {

    public static final String BOOK_TABLE = "library.book";
    public static final String BOOK_BY_TITLE_TABLE = "library.bookByTitle";


    private Session session;

    public BookRepository(Session session) {
        this.session = session;
    }

    public void createBookColumnFamily() {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(BOOK_TABLE).append("(")
                .append("id uuid PRIMARY KEY, ")
                .append("title text,")
                .append("author text,")
                .append("subject text);");

        String query = sb.toString();
        session.execute(query);
    }

    public void createBookByTitleColumnFamily() {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(BOOK_BY_TITLE_TABLE).append("(")
                .append("id uuid, ")
                .append("title text,")
                .append("PRIMARY KEY (title, id));");
        String query = sb.toString();
        session.execute(query);
    }

    public void alterBookColumnFamily(String columnName, String columnType) {
        StringBuilder sb = new StringBuilder("ALTER TABLE ")
                .append(BOOK_TABLE).append(" ADD ")
                .append(columnName).append(" ")
                .append(columnType).append(";");

        String query = sb.toString();
        session.execute(query);
    }

    public void insertBook(Book book) {
        StringBuilder sb = new StringBuilder("INSERT INTO ")
                .append(BOOK_TABLE).append("(id, title, subject, author) ")
                .append("VALUES (").append(book.getId())
                .append(", '").append(book.getTitle()+"'")
                .append(", '").append(book.getSubject()+"'")
                //.append(", '").append(book.getBlurb()+"'")
                .append(", '").append(book.getAuthor()+"'")
                .append(");");
        String query = sb.toString();
        session.execute(query);
    }

    public void insertBookByTitle(Book book) {
        String query = "INSERT INTO " + BOOK_BY_TITLE_TABLE + " (id, title) VALUES (" +
                book.getId() + ",'" + book.getTitle() + "');";
        session.execute(query);
    }

    public List<Book> selectAllBooks() {
        String query = "SELECT * FROM " + BOOK_TABLE;
        ResultSet rs = session.execute(query);
        List<Book> books = rs.all().stream().map(r->rowToBook(r)).collect(Collectors.toList());
       return books;
    }

    public List<Book> selectAllBooksByTitle() {
        String query = "SELECT * FROM " + BOOK_BY_TITLE_TABLE;
        ResultSet rs = session.execute(query);
        List<Book> books = rs.all().stream().map(r->rowToBookByTitle(r)).collect(Collectors.toList());
        return books;
    }

    public List<Book> selectAllBookByTitle(String title) {
        String query = "SELECT * FROM " + BOOK_BY_TITLE_TABLE + " where title =  'Designing Data-Intensive Applications'";
        ResultSet rs = session.execute(query);
        List<Book> books = rs.all().stream().map(r->rowToBookByTitle(r)).collect(Collectors.toList());
        return books;
    }



    public void insertBookBatch(Book book) {

        String query = "BEGIN BATCH INSERT INTO " + BOOK_TABLE
                + " (id, title, subject, author) VALUES ("
                + book.getId() + ", '"
                + book.getTitle() + "' , '"
                + book.getSubject() + "' , '"
                //+ book.getBlurb() + "' , '"
                + book.getAuthor() + "')"
                + " INSERT INTO " + BOOK_BY_TITLE_TABLE
                + " (id, title) VALUES ("
                + book.getId() + ", '"
                + book.getTitle() + "'); "
                + "APPLY BATCH";

            session.execute(query);
    }

    public void deleteColumnFamily(String table) {
        String query = "DROP TABLE IF EXISTS " + table;
        session.execute(query);
    }

    private Book rowToBook(Row row) {
        Book book = new Book();
        book.setId(row.getUUID("id"));
        book.setTitle(row.getString("title"));
        book.setSubject(row.getString("subject"));
        //book.setBlurb(row.getString("blurb"));
        book.setAuthor(row.getString("author"));

        return book;
    }

    private Book rowToBookByTitle(Row row) {
        Book book = new Book();
        book.setId(row.getUUID("id"));
        book.setTitle(row.getString("title"));
        return book;
    }

}
