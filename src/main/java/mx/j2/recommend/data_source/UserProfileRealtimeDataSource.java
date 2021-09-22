package mx.j2.recommend.data_source;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.hystrix.cassandra.StrategyCassandraQueryStringResultCommand;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.*;


public class UserProfileRealtimeDataSource extends BaseDataSource {
    private final static Logger logger = LogManager.getLogger(UserProfileRealtimeDataSource.class);
    private final static String KEYSPACE = "takatak";
    private final static String userProfileTable = "realtime_user_profile";
    private final static String getUserProfileByUserIdQuery = "select * from %s where id='%s';";

    private static CqlSession session;

    public UserProfileRealtimeDataSource() {
        init();
    }

    private void init() {
        try {
            session = CqlSession.builder().addContactEndPoint(new DefaultEndPoint(new InetSocketAddress(Conf.getStrategyCassandraHostUrl(), Conf.getStrategyCassandraHostPort()))).withLocalDatacenter(Conf.getStrategyCassandraDc()).withKeyspace(KEYSPACE).build();
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.toString());
        }
        logger.info(this.getClass().getSimpleName() + " init done!");
    }

    @Trace(dispatcher = true)
    public String getUserProfileByUuId(String uuId) {
        if (MXStringUtils.isEmpty(uuId)) {
            return null;
        }
        String query = String.format(getUserProfileByUserIdQuery, userProfileTable, uuId);
        return new StrategyCassandraQueryStringResultCommand(session, query, "tag_prefer").execute();
    }

    @Trace(dispatcher = true)
    public List<String> getUnRecommendTag(String uuId) {
        String result = getUserProfileByUuId(uuId);
        if(MXStringUtils.isEmpty(result)) {
            return Collections.emptyList();
        }
        List<String> toReturn = new ArrayList<>();
        try {
            JSONObject jo = JSON.parseObject(result);
            if (jo != null) {
                jo.keySet().forEach(key -> {
                    JSONObject score = jo.getJSONObject(key);
                    double toCheck = score.getDouble("score");
                    if (toCheck < 0.0) {
                        toReturn.add(key);
                    }
                });
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
        return toReturn;
    }
}