package codrite.cluster;

import com.datastax.driver.core.Cluster;
import org.junit.After;
import org.junit.Before;

/**
 * No Copyright - only for learning and exploring
 * <p>
 * arnab@codrite.com
 * <p>
 * Creation Date/Time - 6/7/2015 1:13 PM
 */
public class ClusterSetup {

    protected static final String CASSANDRA_HOST = "192.168.56.102";
    protected Cluster cassandraCluster;

    @Before
    public void setupCassandarCluster(){
        cassandraCluster = Cluster.builder().addContactPoint(CASSANDRA_HOST).build();
    }

    @After
    public void cleanup() {
        cassandraCluster.close();
    }

}
