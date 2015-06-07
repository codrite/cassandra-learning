package codrite.table;

import codrite.cluster.ClusterSetup;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import org.junit.Assert;
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
public class InsertAMillionRowsInTransactionTable extends ClusterSetup {

    @Test
    public void insertMillionRows() throws ParseException {
        String SIMPLE_KEYSPACE = "Simple";
        Session session = cassandraCluster.connect(SIMPLE_KEYSPACE);
        final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        final Date date = simpleDateFormat.parse("2015-01-01");

        int rowsToInsert = 1000000;
        int eachInsertBatchSize = rowsToInsert / 50000;
        int batchCurrentCount = 0;
        BatchStatement[] batchStatement = new BatchStatement[eachInsertBatchSize];
        for (int i = 0; i < eachInsertBatchSize; i++) {
            batchStatement[batchCurrentCount] = new BatchStatement();
            for (int j = (i * 50000 + 1); j < ((i + 1) * 50001); j++) {
                final Object[] PARAMS = new Object[]{j, "batch", date};
                final String INSERT_STATEMENT = "Insert into Transaction (id, name, creation) values (?, ?, ?)";
                batchStatement[batchCurrentCount].add(new SimpleStatement(INSERT_STATEMENT, PARAMS));
            }
            batchCurrentCount++;
        }

        for (BatchStatement statement : batchStatement) {
            session.execute(statement);
        }

        String QUERY_TO_COUNT_ROWS = "SELECT COUNT(1) AS COUNT FROM TRANSACTION";
        ResultSet countResult = session.execute(new SimpleStatement(QUERY_TO_COUNT_ROWS));
        Assert.assertEquals(1000020, countResult.all().get(0).getLong("COUNT"));

        session.close();
    }

}
