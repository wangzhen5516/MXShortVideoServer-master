package mx.j2.recommend.data_source;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.hystrix.cassandra.HistoryCassandraQueryMultiRowsResultCommand;
import mx.j2.recommend.hystrix.cassandra.HistoryCassandraQueryStringResultCommand;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.LogTool;
import mx.j2.recommend.util.MXStringUtils;
import mx.j2.recommend.util.bean.BloomInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * @author zhongrenli
 */
@NotThreadSafe
public class HistoryBloomCassandraDataSource extends BaseDataSource {

    private final static Logger logger = LogManager.getLogger(CassandraDataSource.class);

    private final static String HISTORY_KEYSPACE = "takatak_bloom";

    private final static String USER_BLOOM_TABLE = "user_bloom";

    private final static String SELECT_SQL = "select bloom, capacity from %s where user_id='%s' limit 1;";

    private static CqlSession session;

    public HistoryBloomCassandraDataSource() {
        init();
    }

    private void init() {
        try {
            session = CqlSession.builder()
                    .addContactEndPoint(new DefaultEndPoint(new InetSocketAddress(
                            Conf.getHistoryCassandraHostUrl(), Conf.getHistoryCassandraHostPort())))
                    .withLocalDatacenter(Conf.getStrategyCassandraDc())
                    .withKeyspace(HISTORY_KEYSPACE)
                    .build();
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.toString());
        }

        logger.info(this.getClass().getSimpleName() + " init done!");
    }

    @Trace(dispatcher = true)
    private String getUserBloomFromCassandraById(String userId) {
        String query = String.format(SELECT_SQL, USER_BLOOM_TABLE, userId);
        return new HistoryCassandraQueryStringResultCommand(session, query, "bloom").execute();
    }

    @Trace(dispatcher = true)
    private boolean getUserBloomInfoFromCassandraById(String id, BloomInfo info, boolean isVipUser) {
        if (MXStringUtils.isEmpty(id)) {
            return false;
        }
        String query = String.format(SELECT_SQL, USER_BLOOM_TABLE, id);
        Map<String, String> map = new HashMap<>();

        HistoryCassandraQueryMultiRowsResultCommand command = new HistoryCassandraQueryMultiRowsResultCommand(session, query, map, isVipUser);
        boolean executeResult = command.execute();
        boolean executeFinalStatus = executeResult && map.containsKey("bloom") && map.containsKey("capacity");

        if (executeFinalStatus) {
            info.setBloomString(map.get("bloom"));
            try {
                info.setHistorySize(Integer.parseInt(map.get("capacity")));
            } catch (NumberFormatException e) {
                info.setHistorySize(0);
                String message = String.format("[%s][%s] getUserBloomInfoFromCassandraById happen exception: %s", this.getClass().getSimpleName(), query, e.fillInStackTrace());
                LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
            }
        }
        return executeFinalStatus;
    }

    @Trace(dispatcher = true)
    public BloomInfo getBloomInfoFromCassandra(String userId, boolean isVipUser) {
        BloomInfo info = new BloomInfo();
        boolean executeResult = getUserBloomInfoFromCassandraById(userId, info, isVipUser);

        if (!executeResult) {
            return null;
        }

        byte[] bytes = Bytes.fromHexString(info.getBloomString()).array();
        BloomFilter<String> bloom = null;
        try {
            bloom = BloomFilter.readFrom(new ByteArrayInputStream(bytes), (Funnel<String>) (s, primitiveSink) -> primitiveSink.putString(s, Charsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
        }
        info.setBloomFilter(bloom);
        return info;
    }
}
