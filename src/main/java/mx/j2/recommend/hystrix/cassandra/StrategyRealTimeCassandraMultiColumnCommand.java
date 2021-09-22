package mx.j2.recommend.hystrix.cassandra;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.netflix.hystrix.*;
import com.netflix.hystrix.strategy.properties.HystrixPropertiesCommandDefault;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.util.HystrixUtil;
import org.apache.commons.lang.StringUtils;

import java.util.Iterator;

public class StrategyRealTimeCassandraMultiColumnCommand extends HystrixCommand<Long[]> {

    private String query;
    private String[] rowNames;
    private CqlSession session;

    private StrategyRealTimeCassandraMultiColumnCommand() {
        super(HystrixCommand.Setter
                .withGroupKey(HystrixCommandGroupKey.Factory.asKey("StrategyRealTimeCassandraGroup"))
                .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("strategy-realtime-cassandra-pool"))
                .andCommandPropertiesDefaults(
                        HystrixPropertiesCommandDefault.Setter()
                                .withCircuitBreakerEnabled(true)
                                .withCircuitBreakerErrorThresholdPercentage(20)
                                .withCircuitBreakerRequestVolumeThreshold(10)
                                .withCircuitBreakerSleepWindowInMilliseconds(5000)
                                .withExecutionTimeoutInMilliseconds(Conf.getRestClientRequestConfigSocketTimeout())
                                .withExecutionIsolationStrategy(HystrixCommandProperties.ExecutionIsolationStrategy.THREAD))
                .andThreadPoolPropertiesDefaults(
                        HystrixThreadPoolProperties.Setter()
                                .withCoreSize(20)
                                .withMaxQueueSize(40)
                                .withQueueSizeRejectionThreshold(40)));
    }

    public StrategyRealTimeCassandraMultiColumnCommand(CqlSession session, String query, String... rowNames) {
        this();
        this.session = session;
        this.query = query;
        this.rowNames = rowNames;
    }

    @Override
    protected Long[] run() throws Exception {
        ResultSet rs = session.execute(query);
        Iterator<Row> rsIterator = rs.iterator();
        int length = rowNames.length;
        Long[] defaultRes = new Long[length];
        for (int i = 0; i < length; i++) {
            defaultRes[i] = 0L;
        }

        if (length > 0 && rsIterator.hasNext()) {
            Row row = rsIterator.next();
            for (int i = 0; i < rowNames.length; i++) {
                String rowName = rowNames[i];
                if (StringUtils.isEmpty(rowName)) {
                    continue;
                }
                Long result = new Long(row.getLong(rowName));
                defaultRes[i] = result;
            }
            return defaultRes;
        }
        return null;
    }

    @Override
    protected Long[] getFallback() {
        HystrixUtil.logFallback(this);
        return null;
    }
}
