package com.itamar.cassandra.repository;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.itamar.cassandra.entity.Book;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class BookRepository {

    Session session;

    public BookRepository(Session session) {
        this.session = session;
    }

    public void createBookColumnFamily() {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append("library.book").append("(")
                .append("id uuid PRIMARY KEY, ")
                .append("title text,")
                .append("author text,")
                .append("subject text);");

        String query = sb.toString();
        session.execute(query);
    }

    public void createBookByTitleColumnFamily() {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append("library.bookByTitle").append("(")
                .append("id uuid, ")
                .append("title text,")
                .append("PRIMARY KEY (title, id));");
        String query = sb.toString();
        session.execute(query);
    }

    public void alterBookColumnFamily(String columnName, String columnType) {
        StringBuilder sb = new StringBuilder("ALTER TABLE ")
                .append("library.book").append(" ADD ")
                .append(columnName).append(" ")
                .append(columnType).append(";");

        String query = sb.toString();
        session.execute(query);
    }

    public void insertBook(Book book) {
        StringBuilder sb = new StringBuilder("INSERT INTO ")
                .append("library.book").append("(id, title, subject, blurb, author) ")
                .append("VALUES (").append(book.getId())
                .append(", '").append(book.getTitle()+"'")
                .append(", '").append(book.getSubject()+"'")
                .append(", '").append(book.getBlurb()+"'")
                .append(", '").append(book.getAuthor()+"'")
                .append(");");
        String query = sb.toString();
        session.execute(query);
    }

    public List<Book> selectAllBooks() {
        StringBuilder sb =
                new StringBuilder("SELECT * FROM ").append("library.book");

        String query = sb.toString();
        ResultSet rs = session.execute(query);

        List<Book> books = rs.all().stream().map(r->rowToBook(r)).collect(Collectors.toList());

       return books;
    }

    private Book rowToBook(Row row) {
        Book book = new Book();
        book.setId(row.getUUID("id"));
        book.setTitle(row.getString("title"));
        book.setSubject(row.getString("subject"));
        book.setBlurb(row.getString("blurb"));
        book.setAuthor(row.getString("author"));

        return book;
    }

}
