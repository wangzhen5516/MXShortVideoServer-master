package mx.j2.recommend.data_source;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.hystrix.cassandra.CreatorUploadTimeCommand;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CreatorDataSource extends BaseDataSource {
    private final Logger logger = LogManager.getLogger(CreatorDataSource.class);
    private final String KEYSPACE = "medal";
    private final String QUERY_TIME = "select date_time from publish_time where publisher_id = '%s' and date_time > %s;";
    private final String COLUMN_NAME_TIME = "date_time";

    private static CqlSession session;

    public CreatorDataSource() {
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
            e.printStackTrace();
            logger.error(e.toString());
        }
        logger.info(this.getClass().getSimpleName() + " init done!");
    }

    public int checkCreator(String publisherId, Long fromTimestamp) {
        List<Long> timestamps;
        try {
            timestamps = new CreatorUploadTimeCommand(session, String.format(QUERY_TIME, publisherId, fromTimestamp), COLUMN_NAME_TIME).execute();
        } catch (Exception e) {
            System.out.println("can't get res from CreatorDataSource CA!");
            logger.error("can't get res from CreatorDataSource CA!");
            return 0;
        }

        DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
        Set<String> set = new HashSet<>();
        for (Long ts : timestamps) {
            set.add(sdf.format(ts));
        }

        return set.size();
    }
}
