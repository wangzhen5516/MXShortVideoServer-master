package mx.j2.recommend.data_source;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.hystrix.cassandra.PublisherPageCassandraQueryResultCommand;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zhongren.li
 * @date 2021-03-22 10:54
 */
public class PublisherPageCassandraDataSource extends BaseDataSource {
    private final static Logger logger = LogManager.getLogger(PublisherPageCassandraDataSource.class);

    private final static String KEYSPACE = "publisher";

    private static CqlSession session;

    public PublisherPageCassandraDataSource() {
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
            logger.error("init PublisherPageCassandraDataSource error", e);
        }
    }

    public List<String> getVideosOfPublisher(String query) {
        ResultSet rs = new PublisherPageCassandraQueryResultCommand(session, query).execute();
        if (null == rs) {
            return null;
        }

        List<String> publisherIds = new ArrayList<>();
        for (Row row : rs) {
            publisherIds.add(row.getString("video_id"));
        }
        return publisherIds;
    }
}
