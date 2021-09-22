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
 * 从 CA 中读取多行某列的字符串值
 */
public class CassandraQueryRowsCommand extends HystrixCommand<List<String>> {

    private String query;
    private String column;
    private CqlSession session;

    private CassandraQueryRowsCommand() {
        super(HystrixUtil.cassandraSetter);
    }

    public CassandraQueryRowsCommand(CqlSession session, String query, String column) {
        this();
        this.session = session;
        this.query = query;
        this.column = column;
    }

    @Override
    protected List<String> run() throws Exception {
        ResultSet rs = session.execute(query);
        Iterator<Row> rsIterator = rs.iterator();
        List<String> list = new ArrayList<>();

        while (rsIterator.hasNext()) {
            Row row = rsIterator.next();
            list.add(row.getString(column));
        }

        return list;
    }

    @Override
    protected List<String> getFallback() {
        HystrixUtil.logFallback(this);
        return null;
    }
}
