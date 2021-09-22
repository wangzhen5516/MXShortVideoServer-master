package mx.j2.recommend.hystrix;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.netflix.hystrix.HystrixCommand;
import mx.j2.recommend.util.HystrixUtil;

import java.util.Iterator;


public class GetUserPrePubFromCACommand extends HystrixCommand<String> {
    private CqlSession session;
    private String query;
    private String column;

    public GetUserPrePubFromCACommand(CqlSession session, String query, String column) {
        super(HystrixUtil.strategyCassandraSetter);
        this.session = session;
        this.query = query;
        this.column = column;
    }

    @Override
    protected String run() throws Exception {
        ResultSet rs = session.execute(query);
        Iterator<Row> rsIterator = rs.iterator();
        if (rsIterator.hasNext()) {
            Row row = rsIterator.next();
            return row.getString(column);
        }
        return null;
    }
}
