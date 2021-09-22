package mx.j2.recommend.hystrix.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.netflix.hystrix.HystrixCommand;
import com.newrelic.api.agent.NewRelic;
import mx.j2.recommend.util.HystrixUtil;
import mx.j2.recommend.util.OptionalUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author ：zhongrenli
 * @date ：Created in 11:47 上午 2020/10/23
 */
public class HistoryCassandraQueryMultiRowsResultCommand extends HystrixCommand<Boolean> {
    Logger logger = LogManager.getLogger(HistoryCassandraQueryMultiRowsResultCommand.class);

    private String query;
    private Map<String, String> rowNameToResultMap;
    private CqlSession session;
    private boolean isVipUser;

    private HistoryCassandraQueryMultiRowsResultCommand() {
        super(HystrixUtil.historyCassandraSetter);
    }

    public HistoryCassandraQueryMultiRowsResultCommand(CqlSession session, String query,
                                                       Map<String, String> rowNameToResultMap, boolean isVipUser) {
        this();
        this.isVipUser = isVipUser;
        this.session = session;
        this.query = query;
        this.rowNameToResultMap = rowNameToResultMap;
    }

    @Override
    protected Boolean run() throws Exception {
        CompletionStage<AsyncResultSet> asyncResult = session.executeAsync(query);

        int retryTimes = 2;
        int timeout = 100;
        if (this.isVipUser) {
            retryTimes = 1;
            timeout = 600;
        }
        boolean isFinished = true;
        while (retryTimes > 0) {
            try {
                AsyncResultSet asyncResultSet = asyncResult.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
                Iterable<Row> iterable = asyncResultSet.currentPage();

                OptionalUtil.ofNullable(iterable)
                        .getUtil(Iterable::iterator)
                        .ifPresent(rowIterator -> {
                            if (rowIterator.hasNext()) {
                                Row row = rowIterator.next();
                                this.rowNameToResultMap.put("bloom", row.getString("bloom"));
                                this.rowNameToResultMap.put("capacity", String.valueOf(row.getInt("capacity")));
                            }
                        });
                isFinished = true;
                break;
            } catch (TimeoutException e) {
                String msg = String.format("historyBloom, Group: %s, timeout: %s, reason: %s", this.getCommandGroup(), timeout, "Timeout");
                logger.error("run is error", e);
                if (timeout > 100) {
                    NewRelic.noticeError(msg);
                }
            } catch (InterruptedException e) {
                String msg = String.format("historyBloom, Group: %s, timeout: %s, reason: %s", this.getCommandGroup(), timeout, "Interrupted");
                logger.error("run is error", e);
                NewRelic.noticeError(msg);
            } catch (ExecutionException e) {
                String msg = String.format("historyBloom, Group: %s, timeout: %s, reason: %s", this.getCommandGroup(), timeout, "Execution");
                logger.error("run is error", e);
                NewRelic.noticeError(msg);
            } catch (Exception e) {
                String msg = String.format("historyBloom, Group: %s, timeout: %s, reason: %s", this.getCommandGroup(), timeout, "OtherException");
                logger.error("run is error", e);
                NewRelic.noticeError(msg);
            }
            isFinished = false;
            retryTimes--;
            timeout = timeout + 200;
        }
        if (!isFinished) {
            throw new Exception();
        }
        return true;
    }

    @Override
    protected Boolean getFallback() {
        HystrixUtil.logFallback(this);
        return false;
    }
}
