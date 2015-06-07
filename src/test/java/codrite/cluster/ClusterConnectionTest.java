package codrite.cluster;

import com.datastax.driver.core.*;
import org.junit.Assert;
import org.junit.Test;

/**
 * No Copyright - only for learning and exploring
 * <p>
 * arnab@codrite.com
 * <p>
 * Creation Date/Time - 6/7/2015 1:13 PM
 */
public class ClusterConnectionTest extends ClusterSetup {

    @Test
    public void connectToCassandraInstance() {
        Metadata metaDataAboutCluster = cassandraCluster.getMetadata();

        for (Host host : metaDataAboutCluster.getAllHosts()) {
            Assert.assertEquals(CASSANDRA_HOST, host.getAddress().getHostAddress());
        }
    }

}
