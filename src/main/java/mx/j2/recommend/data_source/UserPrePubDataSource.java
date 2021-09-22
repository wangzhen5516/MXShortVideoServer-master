package mx.j2.recommend.data_source;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.hystrix.GetUserPrePubFromCACommand;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class UserPrePubDataSource extends BaseDataSource {
    private static CqlSession realTimeSession;
    private static CqlSession longTermSession;
    private static String REALTIME_KEYSPACE = DefineTool.UserProfile.Tags.KEY_SPACE;
    private static String LONGTERM_KEYSPACE = DefineTool.UserProfile.Tags.KEY_SPACE;
    private static Logger logger = LogManager.getLogger(UserPrePubDataSource.class);


    public UserPrePubDataSource() {
        init();
    }

    public void init() {
        try {
            longTermSession = CqlSession.builder()
                    .addContactEndPoint(new DefaultEndPoint(new InetSocketAddress(
                            Conf.getStrategyTagCassandraHost(), Conf.getStrategyTagCassandraPort())))
                    .withLocalDatacenter(Conf.getStrategyTagCassandraDc())
                    .withKeyspace(REALTIME_KEYSPACE)
                    .build();
            realTimeSession = CqlSession.builder()
                    .addContactEndPoint(new DefaultEndPoint(new InetSocketAddress(
                            Conf.getStrategyCassandraHostUrl(), Conf.getStrategyCassandraHostPort())))
                    .withLocalDatacenter(Conf.getStrategyCassandraDc())
                    .withKeyspace(LONGTERM_KEYSPACE)
                    .build();
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.toString());
        }
        logger.info(this.getClass().getSimpleName() + " init done!");
    }

    public List<String> getPubListFromCA(String query, String column, int maxNum, char flag) {
        List<String> pubList = null;
        switch (flag) {
            case 'r':
                String rString = new GetUserPrePubFromCACommand(realTimeSession, query, column).execute();
                pubList = parseRealTime(rString, maxNum);
                break;
            case 'l':
                String lString = new GetUserPrePubFromCACommand(longTermSession, query, column).execute();
                pubList = parseLongTerm(lString, maxNum);
                break;
            default:
        }
        return pubList;
    }

    public List<String> getRealPubListFromCA(String query, String column, int maxNum) {
        String rString = new GetUserPrePubFromCACommand(realTimeSession, query, column).execute();
        if (MXJudgeUtils.isEmpty(rString)) {
            return Collections.emptyList();
        }
        List<JSONObject> array = JSONArray.parseArray(rString).toJavaList(JSONObject.class);
        List<String> pubList = new ArrayList<>();
        for (JSONObject jsonObject : array) {
            pubList.add(jsonObject.getString("id"));
        }
        return pubList;
    }

    private List<String> parseRealTime(String s, int maxNum) {
        if (MXStringUtils.isEmpty(s)) {
            return null;
        }
        logger.info(s);
        JSONObject obj = (JSONObject) JSONObject.parse(s);
        int num = Math.min(obj.keySet().size(), maxNum);
        List<String> pubList = new ArrayList<>();
        List<String> protoList = new ArrayList<>();
        obj.keySet().forEach((o) -> protoList.add(o));
        Collections.sort(protoList, (s1, s2) -> (obj.getJSONObject(s2).getIntValue("score") - obj.getJSONObject(s1).getIntValue("score")));
        for (int i = 0; i < num; i++) {
            pubList.add(protoList.get(i));
        }
        logger.info(pubList);
        return pubList;
    }

    private List<String> parseLongTerm(String s, int maxNum) {
        if (MXStringUtils.isEmpty(s)) {
            return null;
        }
        JSONObject obj = (JSONObject) JSONObject.parse(s);
        int num = Math.min(obj.keySet().size(), maxNum);
        List<String> pubList = new ArrayList<>();

        List<String> protoList = new ArrayList<>();
        obj.keySet().forEach((o) -> protoList.add(o));
        Collections.sort(protoList, (s1, s2) -> (int) (obj.getDoubleValue(s2) - obj.getDoubleValue(s1)));
        for (int i = 0; i < num; i++) {
            pubList.add(protoList.get(i));
        }
        return pubList;
    }
}

