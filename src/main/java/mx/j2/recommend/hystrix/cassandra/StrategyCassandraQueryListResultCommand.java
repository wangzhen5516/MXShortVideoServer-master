package mx.j2.recommend.hystrix.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.netflix.hystrix.HystrixCommand;
import com.newrelic.api.agent.NewRelic;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.HystrixUtil;
import mx.j2.recommend.util.LogTool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.List;

/**
 * @author ：zhongrenli
 * @date ：Created in 11:47 上午 2020/10/23
 */
public class StrategyCassandraQueryListResultCommand extends HystrixCommand<List<String>> {
    private static Logger logger = LogManager.getLogger(StrategyCassandraQueryListResultCommand.class);

    private String query;
    private String rowName;
    private CqlSession session;

    private StrategyCassandraQueryListResultCommand() {
        super(HystrixUtil.strategyCassandraSetter);
    }

    public StrategyCassandraQueryListResultCommand(CqlSession session, String query, String rowName) {
        this();
        this.session = session;
        this.query = query;
        this.rowName = rowName;
    }

    @Override
    protected List<String> run() throws Exception {
        ResultSet rs = session.execute(query);
        Iterator<Row> rsIterator = rs.iterator();

        if (rsIterator.hasNext()) {
            Row row = rsIterator.next();
            return row.getList(rowName, String.class);
        }

        return null;
    }

    @Override
    protected List<String> getFallback() {
        HystrixUtil.logFallback(this);

        Throwable e = this.getExecutionException();
        String message = String.format("cassandra happen exception: %s", e.fillInStackTrace());
        LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        NewRelic.noticeError("load history from cassandra failed!");
        return null;
    }
}
