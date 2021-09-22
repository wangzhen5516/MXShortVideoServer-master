package mx.j2.recommend.data_source;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.hystrix.cassandra.PublisherCassandraQueryStringResultCommand;
import mx.j2.recommend.util.MXStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;

/**
 * @author qiqi
 * @date 2021-01-14 10:54
 */
public class PublisherCassandraDataSource extends BaseDataSource {
    private final static Logger logger = LogManager.getLogger(PublisherCassandraDataSource.class);

    private final static String KEYSPACE = "takatak";

    private final static String PUB_VIDEO_NUM_TABLE = "pub_video_num";

    private final static String GET_RESULT_BY_ID = "select * from %s where id='%s';";

    private static CqlSession session;

    public PublisherCassandraDataSource() {
        init();
    }

    private void init() {
        try {
            session = CqlSession.builder()
                    .addContactEndPoint(new DefaultEndPoint(new InetSocketAddress(
                            Conf.getPublisherCassandraHostUrl(), Conf.getPublisherCassandraHostPort())))
                    .withLocalDatacenter(Conf.getStrategyCassandraDc())
                    .withKeyspace(KEYSPACE)
                    .build();
        } catch (Exception e) {
            logger.error("init publisherca error", e);
        }
    }


    public Integer getVideoNumOfPub(String publisherId, String type) {
        String query = String.format(GET_RESULT_BY_ID, PUB_VIDEO_NUM_TABLE, publisherId);
        String rowType = "";
        if ("public".equals(type)) {
            rowType = "public_num";
        } else if ("private".equals(type)) {
            rowType = "private_num";
        }
        if (MXStringUtils.isBlank(rowType)) {
            return null;
        }
        String videoNum = new PublisherCassandraQueryStringResultCommand(session, query, rowType).execute();
        if (MXStringUtils.isBlank(videoNum) || !MXStringUtils.isNumeric(videoNum)) {
            return null;
        }
        return Integer.parseInt(videoNum);
    }
}
