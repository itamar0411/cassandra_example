import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.itamar.cassandra.connector.CassandraConnection;
import com.itamar.cassandra.repository.KeyspaceRepository;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestKeyspaceRepository {

    private CassandraConnection cassandraConnection = null;
    private Session session = null;
    private KeyspaceRepository keyspaceRepository = null;

    @Before
    public void connect() {
        cassandraConnection = new CassandraConnection();
        cassandraConnection.connect("localhost", 9042);
        this.session = cassandraConnection.getSession();
        keyspaceRepository = new KeyspaceRepository(session);
    }

    @Test
    public void testKeyspaceCreation() {
        String keyspaceName = "library";
        keyspaceRepository.createKeyspace(keyspaceName, "SimpleStrategy", 1);

        ResultSet result =
                session.execute("SELECT * FROM system_schema.keyspaces;");

        List<String> matchedKeyspaces = result.all()
                .stream()
                .filter(r -> r.getString(0).equals(keyspaceName.toLowerCase()))
                .map(r -> r.getString(0))
                .collect(Collectors.toList());

        assertEquals(matchedKeyspaces.size(), 1);
        assertTrue(matchedKeyspaces.get(0).equals(keyspaceName.toLowerCase()));
    }


    @After
    public void closeConnection() {
        cassandraConnection.close();
    }
}
