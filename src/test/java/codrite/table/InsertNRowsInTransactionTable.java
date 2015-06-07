package codrite.table;

import codrite.cluster.ClusterSetup;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * No Copyright - only for learning and exploring
 * <p>
 * arnab@codrite.com
 * <p>
 * Creation Date/Time - 6/7/2015 1:13 PM
 */
public class InsertNRowsInTransactionTable extends ClusterSetup {

    public static final int ROWS_TO_INSERT = 200000;
    public static final int BATCH_SIZE = 10000;
    public static final String SIMPLE_KEYSPACE = "Simple";

    @Before
    public void clearTransactionTable() {
        Session session = cassandraCluster.connect(SIMPLE_KEYSPACE);
        session.execute("truncate transaction");
        session.close();
    }

    @Test
    public void insertMillionRows() throws ParseException {
        Session session = cassandraCluster.connect(SIMPLE_KEYSPACE);
        final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        final Date date = simpleDateFormat.parse("2015-01-01");

        int numberOfBatches = (ROWS_TO_INSERT / BATCH_SIZE);
        int batchCurrentCount = 0;
        BatchStatement[] batchStatement = new BatchStatement[numberOfBatches];

        do {
            batchStatement[batchCurrentCount] = new BatchStatement();
            int rowsForBatch = ((batchCurrentCount + 1) * BATCH_SIZE) + 1;
            for (int j = (batchCurrentCount * BATCH_SIZE + 1); j < rowsForBatch; j++) {
                final Object[] PARAMS = new Object[]{j, "batch", date};
                final String INSERT_STATEMENT = "Insert into Transaction (id, name, creation) values (?, ?, ?)";
                batchStatement[batchCurrentCount].add(new SimpleStatement(INSERT_STATEMENT, PARAMS));
            }
            batchCurrentCount++;
        } while ( batchCurrentCount < numberOfBatches );

        for (BatchStatement statement : batchStatement) {
            session.execute(statement);
        }

        String QUERY_TO_COUNT_ROWS = "SELECT COUNT(1) AS COUNT FROM TRANSACTION";
        ResultSet countResult = session.execute(new SimpleStatement(QUERY_TO_COUNT_ROWS));
        int additionalCount = ((ROWS_TO_INSERT / BATCH_SIZE) + 1);
        Assert.assertEquals(ROWS_TO_INSERT + additionalCount, countResult.all().get(0).getLong("COUNT"));

        session.close();
    }

}
