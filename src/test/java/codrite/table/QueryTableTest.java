package codrite.table;

import codrite.cluster.ClusterSetup;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.junit.Assert;
import org.junit.Test;

/**
 * No Copyright - only for learning and exploring
 * <p>
 * arnab@codrite.com
 * <p>
 * Creation Date/Time - 6/7/2015 1:13 PM
 */
public class QueryTableTest extends ClusterSetup {

    public static final String SIMPLE_KEYSPACE = "Simple";

    @Test
    public void queryTransactionTableInSimpleKeyspace() {
        Session session = cassandraCluster.connect(SIMPLE_KEYSPACE);

        ResultSet countResult = session.execute("select COUNT(1) AS COUNT from transaction");
        Assert.assertEquals(0, countResult.all().get(0).getLong("COUNT"));

        session.close();
    }

}
