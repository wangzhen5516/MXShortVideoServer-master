package mx.j2.recommend.hystrix.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.netflix.hystrix.*;
import com.netflix.hystrix.strategy.properties.HystrixPropertiesCommandDefault;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.util.HystrixUtil;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CreatorUploadTimeCommand extends HystrixCommand<List<Long>> {

    private String query;
    private CqlSession session;
    private String columnName;

    private CreatorUploadTimeCommand() {
        super(HystrixCommand.Setter
                .withGroupKey(HystrixCommandGroupKey.Factory.asKey("PublisherPageCassandraGroup"))
                .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("publisher-page-cassandra-pool"))
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

    public CreatorUploadTimeCommand(CqlSession session, String query, String columnName) {
        this();
        this.session = session;
        this.query = query;
        this.columnName = columnName;
    }

    @Override
    protected List<Long> run() throws Exception {
        ResultSet rs = session.execute(query);

        Iterator<Row> rsIterator = rs.iterator();
        List<Long> ts = new ArrayList<>();
        while (rsIterator.hasNext()) {
            Row row = rsIterator.next();
            Long tt = new Long(row.getLong(columnName));
            ts.add(tt);
        }
        return ts;
    }

    @Override
    protected List<Long> getFallback() {
        HystrixUtil.logFallback(this);
        return null;
    }
}
