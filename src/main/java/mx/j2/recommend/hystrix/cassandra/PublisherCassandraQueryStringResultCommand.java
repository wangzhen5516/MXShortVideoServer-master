package mx.j2.recommend.hystrix.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.netflix.hystrix.HystrixCommand;
import mx.j2.recommend.util.HystrixUtil;

import java.util.Iterator;

/**
 * @author qiqi
 * @date 2021-01-15 10:42
 */
public class PublisherCassandraQueryStringResultCommand extends HystrixCommand<String> {

    private String query;
    private String columnName;
    private CqlSession session;

    private PublisherCassandraQueryStringResultCommand() {
        super(HystrixUtil.publisherCassandraSetter);
    }

    public PublisherCassandraQueryStringResultCommand(CqlSession session, String query, String columnName) {
        this();
        this.session = session;
        this.query = query;
        this.columnName = columnName;
    }

    @Override
    protected String run() throws Exception {
        ResultSet rs = session.execute(query);
        Iterator<Row> rsIterator = rs.iterator();

        if (rsIterator.hasNext()) {
            Row row = rsIterator.next();
            return row.getString(columnName);
        }
        return null;
    }

    @Override
    protected String getFallback() {
        HystrixUtil.logFallback(this);
        return "";
    }
}

