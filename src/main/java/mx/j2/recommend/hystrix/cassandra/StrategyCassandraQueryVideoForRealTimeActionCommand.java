package mx.j2.recommend.hystrix.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.netflix.hystrix.HystrixCommand;
import mx.j2.recommend.util.HystrixUtil;

import java.util.Iterator;

/**
 * @author ：DuoZhao
 * @date ：Created in 13:45 上午 2020/12/05
 */
public class StrategyCassandraQueryVideoForRealTimeActionCommand extends HystrixCommand<String> {

    private String query;
    private String rowName;
    private CqlSession session;

    private StrategyCassandraQueryVideoForRealTimeActionCommand() {
        super(HystrixUtil.strategyEsSetter);
    }

    public StrategyCassandraQueryVideoForRealTimeActionCommand(CqlSession session, String query, String rowName) {
        this();
        this.session = session;
        this.query = query;
        this.rowName = rowName;
    }

    @Override
    protected String run() throws Exception {
        ResultSet rs = session.execute(query);
        Iterator<Row> rsIterator = rs.iterator();

        if (rsIterator.hasNext()) {
            Row row = rsIterator.next();
            return row.getString(rowName);
        }
        return null;
    }

    @Override
    protected String getFallback() {
        HystrixUtil.logFallback(this);
        return null;
    }
}
