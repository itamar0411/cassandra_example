import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.utils.UUIDs;
import com.itamar.cassandra.connector.CassandraConnection;
import com.itamar.cassandra.entity.Book;
import com.itamar.cassandra.repository.BookRepository;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestBookRepository {
    private CassandraConnection cassandraConnection = null;
    private Session session = null;
    private BookRepository bookRepository = null;

    @Before
    public void connect() {
        cassandraConnection = new CassandraConnection();
        cassandraConnection.connect("localhost", 9042);
        this.session = cassandraConnection.getSession();
        bookRepository = new BookRepository(session);
    }

    @Test
    public void testBookColumnFamilyCreation() {
        bookRepository.createBookColumnFamily();

        ResultSet result = session.execute(
                "SELECT * FROM "  + "library.book;");

        List<String> columnNames =
                result.getColumnDefinitions().asList().stream()
                        .map(cl -> cl.getName())
                        .collect(Collectors.toList());

        assertEquals(columnNames.size(), 4);
        assertTrue(columnNames.contains("id"));
        assertTrue(columnNames.contains("title"));
        assertTrue(columnNames.contains("author"));
        assertTrue(columnNames.contains("subject"));
    }

    @Test
    public void testCreateBookByTitleColumnFamily() {
        bookRepository.createBookByTitleColumnFamily();
        ResultSet result = session.execute(
                "SELECT * FROM "  + "library.bookByTitle;");

        List<String> columnNames =
                result.getColumnDefinitions().asList().stream()
                        .map(cl -> cl.getName())
                        .collect(Collectors.toList());

        assertEquals(columnNames.size(), 2);
        assertTrue(columnNames.contains("id"));
        assertTrue(columnNames.contains("title"));
    }

//    @Test
//    public void testAlterBookColumnFamily() {
//        // creating it, so this test will work standalone
//        bookRepository.createBookColumnFamily();
//
//        bookRepository.alterBookColumnFamily("blurb", "text");
//
//        ResultSet resultSet = session.execute("select * from library.book;");
//
//        boolean columnExists = resultSet.getColumnDefinitions().asList().stream().anyMatch(c->c.getName().equals("blurb"));
//
//        assertTrue(columnExists);
//    }

    @Test
    public void testInsertBooks() {
        bookRepository.createBookColumnFamily();

        Book book1 = new Book();
        book1.setId(UUIDs.timeBased());
        book1.setTitle("Designing Data-Intensive Applications");
        book1.setAuthor("Martin Kleppmann");
        //book1.setBlurb("Principles on how to build data-intensive applications");
        book1.setSubject("Software Systems");

        Book book2 = new Book();
        book2.setId(UUIDs.timeBased());
        book2.setTitle("Design It");
        book2.setAuthor("Michael Keeling");
        //book2.setBlurb("Design it like you mean it");
        book2.setSubject("Software Systems");

        bookRepository.insertBook(book1);
        bookRepository.insertBook(book2);

        List<Book> books = bookRepository.selectAllBooks();

        assertEquals(books.size(), 2);

    }

    @Test
    public void testInsetBooksByTitle() {
        bookRepository.createBookByTitleColumnFamily();

        Book book = new Book();
        book.setId(UUIDs.timeBased());
        book.setTitle("Designing Data-Intensive Applications");

        bookRepository.insertBookByTitle(book);

        List<Book> books = bookRepository.selectAllBooksByTitle();

        assertEquals(books.size(), 1);
    }

    @Test
    public void testSelectBookByTitle() {

        bookRepository.createBookByTitleColumnFamily();


        List<Book> books = bookRepository.selectAllBookByTitle("Designing Data-Intensive Applications");

        assertTrue(books.size()>0);
    }

    @Test
    public void testBatchInsert() {
        bookRepository.createBookColumnFamily();
        bookRepository.createBookByTitleColumnFamily();

        Book book1 = new Book();
        book1.setId(UUIDs.timeBased());
        book1.setTitle("Designing Data-Intensive Applications");
        book1.setAuthor("Martin Kleppmann");
        //book1.setBlurb("Principles on how to build data-intensive applications");
        book1.setSubject("Software Systems");

        bookRepository.insertBookBatch(book1);

        List<Book> books = bookRepository.selectAllBooks();
        List<Book> booksByTitle = bookRepository.selectAllBooksByTitle();

        assertEquals(books.get(0).getTitle(), booksByTitle.get(0).getTitle());


    }

    @Test(expected = InvalidQueryException.class)
    public void deleteColumFamily() {
        bookRepository.createBookColumnFamily();
        bookRepository.createBookByTitleColumnFamily();
        bookRepository.deleteColumnFamily(BookRepository.BOOK_TABLE);
        bookRepository.deleteColumnFamily(BookRepository.BOOK_BY_TITLE_TABLE);

        session.execute("SELECT * FROM " + BookRepository.BOOK_TABLE);
    }

    @After
    public void closeConnection() {
        cassandraConnection.close();
    }
}
