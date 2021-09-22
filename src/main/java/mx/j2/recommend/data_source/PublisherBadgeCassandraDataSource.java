package mx.j2.recommend.data_source;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.hystrix.cassandra.CassandraQueryColumnsCommand;
import mx.j2.recommend.hystrix.cassandra.CassandraQueryColumnsObjectCommand;
import mx.j2.recommend.hystrix.cassandra.PublisherCassandraQueryStringResultCommand;
import mx.j2.recommend.util.MXCollectionUtils;
import mx.j2.recommend.util.MXStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xiang.zhou
 * @date 2021-05-08 10:54
 */
public class PublisherBadgeCassandraDataSource extends BaseDataSource {
    private final static Logger logger = LogManager.getLogger(PublisherBadgeCassandraDataSource.class);

    private final static String KEYSPACE = "medal";

    private final static String PUB_VIDEO_NUM_TABLE = "publish_state";

    private final static String GET_RESULT_BY_ID = "select max_days,max_weeks,total_days from %s where publisher_id='%s';";

    private final static String GET_TIME_FROM_RECORD = "select online_time from %s where publisher_id='%s' and %s=%d";

    private static CqlSession session;

    public PublisherBadgeCassandraDataSource() {
        init();
    }

    private void init() {
        try {
            session = CqlSession.builder()
                    .addContactEndPoint(new DefaultEndPoint(new InetSocketAddress(
                            Conf.getPublisherPageCassandraHost(), Conf.getPublisherPageCassandraPort())))
                    .withLocalDatacenter(Conf.getPublisherPageCassandraDc())
                    .withKeyspace(KEYSPACE)
                    .build();
        } catch (Exception e) {
            logger.error("init badge ca error", e);
        }
    }


    public List<Object> getBadgeCount(String publisherId) {
        List<String> cols = new ArrayList<>();
        cols.add("max_days");
        cols.add("max_weeks");
        cols.add("total_days");

        String query = String.format(GET_RESULT_BY_ID, PUB_VIDEO_NUM_TABLE, publisherId);
        return new CassandraQueryColumnsObjectCommand(session, query, cols).execute();
    }

    public long getTime(String publisherId, String daysOrWeeks, String totalOrContinuous, int days) {
        List<String> cols = new ArrayList<>();
        cols.add("online_time");
        String query = String.format(GET_TIME_FROM_RECORD, "publish_"+daysOrWeeks+"_"+totalOrContinuous, publisherId, daysOrWeeks, days);
        List<Object> rets = new CassandraQueryColumnsObjectCommand(session, query, cols).execute();
        if(MXCollectionUtils.isNotEmpty(rets)) {
            return Long.parseLong(rets.get(0).toString());
        }
        return 0;
    }
}
