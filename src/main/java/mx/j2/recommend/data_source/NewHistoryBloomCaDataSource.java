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
 * @author qiqi
 * @date 2020-12-24 19:53
 */
@NotThreadSafe
public class NewHistoryBloomCaDataSource extends BaseDataSource {

    Logger logger = LogManager.getLogger(NewHistoryBloomCaDataSource.class);

    private static final String HISTORY_KEYSPACE = "takatak_bloom";
    private static final String TABLE_NAME = "user_bloom_%s";
    private static final String SELECT_SQL = "select bloom, capacity from %s where user_id='%s' limit 1;";
    private static CqlSession session;

    public NewHistoryBloomCaDataSource() {
        init();
    }

    private void init() {
        try {
            session = CqlSession.builder()
                    .addContactEndPoint(new DefaultEndPoint(new InetSocketAddress(
                            Conf.getNewHistoryCaHostUrl(), Conf.getNewHistoryCaHostPort())))
                    .withLocalDatacenter(Conf.getNewBloomCassandraDc())
                    .withKeyspace(HISTORY_KEYSPACE)
                    .build();
        } catch (Exception e) {
            logger.error("init newHistoryCa error", e);
        }
        logger.info("init newHistoryCa done.");
    }


    @Trace(dispatcher = true)
    private boolean getUserBloomInfoFromCassandraById(String id, BloomInfo info, boolean isVipUser) {
        if (MXStringUtils.isEmpty(id)) {
            return false;
        }
        int hash = id.hashCode() % 1000;
        if (hash < 0) {
            hash += 1000;
        }
        String tableName = String.format(TABLE_NAME, String.valueOf(hash));
        String query = String.format(SELECT_SQL, tableName, id);
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
                logger.error("setHistorySize is error", e);
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
            logger.error("BloomFilter.readFrom is error", e);
        }
        info.setBloomFilter(bloom);
        return info;
    }
}
