package mx.j2.recommend.hystrix.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.netflix.hystrix.HystrixCommand;
import mx.j2.recommend.util.HystrixUtil;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 从 CA 中读取某行多列的字符串值
 */
public class CassandraQueryColumnsCommand extends HystrixCommand<List<String>> {

    private String query;
    private List<String> columns;
    private CqlSession session;

    private CassandraQueryColumnsCommand() {
        super(HystrixUtil.cassandraSetter);
    }

    public CassandraQueryColumnsCommand(CqlSession session, String query, List<String> columns) {
        this();
        this.session = session;
        this.query = query;
        this.columns = columns;
    }

    @Override
    protected List<String> run() throws Exception {
        ResultSet rs = session.execute(query);
        Iterator<Row> rsIterator = rs.iterator();
        List<String> list = new ArrayList<>();

        if (rsIterator.hasNext()) {
            Row row = rsIterator.next();

            for (String columnName : columns) {
                list.add(row.getString(columnName));
            }
        }

        return list;
    }

    @Override
    protected List<String> getFallback() {
        HystrixUtil.logFallback(this);
        return null;
    }
}
