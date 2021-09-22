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
public class PublisherPageCassandraQueryResultCommand extends HystrixCommand<ResultSet> {

    private String query;
    private CqlSession session;

    private PublisherPageCassandraQueryResultCommand() {
        super(HystrixUtil.PUBLISHER_PAGE_CASSANDRA_SETTER);
    }

    public PublisherPageCassandraQueryResultCommand(CqlSession session, String query) {
        this();
        this.session = session;
        this.query = query;
    }

    @Override
    protected ResultSet run() throws Exception {
        ResultSet rs = session.execute(query);
        Iterator<Row> rsIterator = rs.iterator();

        if (rsIterator.hasNext()) {
            return rs;
        }
        return null;
    }

    @Override
    protected ResultSet getFallback() {
        HystrixUtil.logFallback(this);
        return null;
    }
}

