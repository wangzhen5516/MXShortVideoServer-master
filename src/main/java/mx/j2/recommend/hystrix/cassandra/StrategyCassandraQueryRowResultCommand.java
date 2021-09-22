package mx.j2.recommend.hystrix.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.netflix.hystrix.HystrixCommand;
import mx.j2.recommend.util.HystrixUtil;

/**
 * @Author: xiaoling.zhu
 * @Date: 2020-12-22
 */

public class StrategyCassandraQueryRowResultCommand extends HystrixCommand<Row> {
    private String query;
    private String rowName;
    private CqlSession session;

    public StrategyCassandraQueryRowResultCommand(CqlSession session, String query) {
        super(HystrixUtil.strategyCassandraSetter);
        this.session = session;
        this.query = query;
    }

    @Override
    protected Row run() throws Exception {
        ResultSet rs = session.execute(query);
        if(rs!=null){
            return rs.one();
        }
        return null;
    }

    @Override
    protected Row getFallback() {
        HystrixUtil.logFallback(this);
        return null;
    }
}
