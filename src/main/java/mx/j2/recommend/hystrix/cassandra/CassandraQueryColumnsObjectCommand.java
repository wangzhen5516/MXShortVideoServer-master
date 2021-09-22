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
public class CassandraQueryColumnsObjectCommand extends HystrixCommand<List<Object>> {

    private String query;
    private List<String> columns;
    private CqlSession session;

    private CassandraQueryColumnsObjectCommand() {
        super(HystrixUtil.cassandraSetter);
    }

    public CassandraQueryColumnsObjectCommand(CqlSession session, String query, List<String> columns) {
        this();
        this.session = session;
        this.query = query;
        this.columns = columns;
    }

    @Override
    protected List<Object> run() throws Exception {
        ResultSet rs = session.execute(query);
        Iterator<Row> rsIterator = rs.iterator();
        List<Object> list = new ArrayList<>();

        if (rsIterator.hasNext()) {
            Row row = rsIterator.next();
            for (String columnName : columns) {
                list.add(row.getObject(columnName));
            }
        }

        return list;
    }

    @Override
    protected List<Object> getFallback() {
        HystrixUtil.logFallback(this);
        return null;
    }
}
