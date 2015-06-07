package codrite.keyspace;

import codrite.cluster.ClusterSetup;
import com.datastax.driver.core.*;
import org.junit.Test;

/**
 * No Copyright - only for learning and exploring
 * <p>
 * arnab@codrite.com
 * <p>
 * Creation Date/Time - 6/7/2015 1:13 PM
 */
public class KeyspaceConnectionTest extends ClusterSetup {

    public static final String SIMPLE_KEYSPACE = "Simple";

    @Test
    public void connectToSimpleKeyspace() {
        Session session = cassandraCluster.connect(SIMPLE_KEYSPACE);
        session.close();
    }

}
