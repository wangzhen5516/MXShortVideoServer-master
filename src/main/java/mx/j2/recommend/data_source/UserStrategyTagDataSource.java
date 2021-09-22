package mx.j2.recommend.data_source;

import com.alibaba.fastjson.JSONObject;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.UserProfile;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.hystrix.GetUserPrePubFromCACommand;
import mx.j2.recommend.hystrix.cassandra.StrategyCassandraQueryStringResultCommand;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXCollectionUtils;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * 用户个性化标签数据 ml_tag 数据源
 *
 * @author zhongrenli
 */
public class UserStrategyTagDataSource extends BaseDataSource {
    private final static Logger logger = LogManager.getLogger(UserStrategyTagDataSource.class);
    private final static String KEYSPACE = DefineTool.UserProfile.Tags.KEY_SPACE;
    private final static String QUERY = "select * from %s where uuid='%s';";
    private final static String COLUMN = "tag_profile";
    private final static String COLUMN_LANGUAGE_AND_INTEREST = "choose";
    private final static String KEYSPACE_LANGUAGE_AND_INTEREST = "takatak_language_interest";
    private final static String TABLE_LANGUAGE_AND_INTEREST = "language_interest";

    private static CqlSession session;
    private static CqlSession session_LANGUAGE_AND_INTEREST;


    public UserStrategyTagDataSource() {
        init();
    }

    private void init() {
        try {
            session = CqlSession.builder()
                    .addContactEndPoint(new DefaultEndPoint(new InetSocketAddress(
                            Conf.getStrategyTagCassandraHost(), Conf.getStrategyTagCassandraPort())))
                    .withLocalDatacenter(Conf.getStrategyTagCassandraDc())
                    .withKeyspace(KEYSPACE)
                    .build();
            session_LANGUAGE_AND_INTEREST = CqlSession.builder()
                    .addContactEndPoint(new DefaultEndPoint(new InetSocketAddress(
                            Conf.getStrategyTagCassandraHost(), Conf.getStrategyTagCassandraPort())))
                    .withLocalDatacenter(Conf.getStrategyTagCassandraDc())
                    .withKeyspace(KEYSPACE_LANGUAGE_AND_INTEREST)
                    .build();
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.toString());
        }
        logger.info(this.getClass().getSimpleName() + " init done!");
    }

    /**
     * 必须通过此接口拿标签信息
     *
     * @return 返回副本，随便改
     */
    @Nullable
    @Deprecated
    public List<UserProfile.Tag> getTags(BaseDataCollection dc) {
        if (MXStringUtils.isEmpty(dc.client.user.uuId)) {
            return null;
        }
        if (MXStringUtils.isEmpty(dc.tagTableName)) {
            return null;
        }

        /*
         * 第一次拉取，且只拉一次
         */
        String query = String.format(QUERY, dc.tagTableName, dc.client.user.uuId);
        String tagsStr = new StrategyCassandraQueryStringResultCommand(session, query, COLUMN).execute();
        List<UserProfile.Tag> tags = parseTags(tagsStr);
        if (MXCollectionUtils.isEmpty(tags)) {
            return null;
        }
        dc.userLongTagSet.addAll(tags);
        return new ArrayList<>(tags);
    }

    /**
     * 必须通过此接口拿标签信息
     *
     * @return 返回副本，随便改
     */
    @Nullable
    @Deprecated
    public List<UserProfile.Tag> getCategories(BaseDataCollection dc) {
        if (MXStringUtils.isEmpty(dc.client.user.uuId)) {
            return null;
        }
        if (MXStringUtils.isEmpty(dc.tagTableName)) {
            return null;
        }

        /*
         * 第一次拉取，且只拉一次
         */
        String query = String.format(QUERY, dc.tagTableName, dc.client.user.uuId);
        String tagsStr = new StrategyCassandraQueryStringResultCommand(session, query, COLUMN).execute();
        List<UserProfile.Tag> categories = parsCategories(tagsStr);
        if (MXCollectionUtils.isEmpty(categories)) {
            return null;
        }
        dc.userLongCategorySet.addAll(categories);
        return new ArrayList<>(categories);
    }

    public List<String> getRealPubListFromCA(String query, String column, int maxNum) {
        String rString = new GetUserPrePubFromCACommand(session, query, column).execute();
        if (MXJudgeUtils.isEmpty(rString)) {
            return Collections.emptyList();
        }
        JSONObject jsonObject = JSONObject.parseObject(rString);
        List<String> pubList = new ArrayList<>();
        if (jsonObject != null && jsonObject.size() > 0) {
            pubList.addAll(jsonObject.keySet());
        }
        return pubList;
    }

    /**
     * 解析标签集合
     */
    private List<UserProfile.Tag> parseTags(String tagsStr) {
        if (MXStringUtils.isEmpty(tagsStr)) {
            return null;
        }

        JSONObject tagMap = JSONObject.parseObject(tagsStr);
        if (tagMap == null) {
            return null;
        }

        List<UserProfile.Tag> tags = new ArrayList<>();
        float tagScoreIt;

        for (String tagNameIt : tagMap.keySet()) {
            tagScoreIt = tagMap.getFloatValue(tagNameIt);
            tags.add(new UserProfile.Tag(tagNameIt, tagScoreIt));
        }

        return tags;
    }

    /**
     * 解析标签集合
     */
    private List<UserProfile.Tag> parsCategories(String categoriesStr) {
        if (MXStringUtils.isEmpty(categoriesStr)) {
            return null;
        }

        JSONObject tagMap = JSONObject.parseObject(categoriesStr);
        if (tagMap == null) {
            return null;
        }

        List<UserProfile.Tag> tags = new ArrayList<>();
        float tagScoreIt;

        for (String tagNameIt : tagMap.keySet()) {
            tagScoreIt = tagMap.getFloatValue(tagNameIt);
            tags.add(new UserProfile.Tag(tagNameIt, tagScoreIt));
        }

        return tags;
    }

    public List<String> getLanguageAndInterestFromCA(String uuid) {
        if (MXStringUtils.isEmpty(uuid)) {
            return null;
        }
        String query = String.format(QUERY, TABLE_LANGUAGE_AND_INTEREST, uuid);
        String resString = new StrategyCassandraQueryStringResultCommand(session_LANGUAGE_AND_INTEREST, query, COLUMN_LANGUAGE_AND_INTEREST).execute();
        if (MXStringUtils.isEmpty(resString)) {
            return null;
        }
        List<String> res = parseList(resString);
        if (MXCollectionUtils.isEmpty(res)) {
            return null;
        }
        return res;
    }

    @Trace(dispatcher = true)
    public String getStrategyOutPutFromCassandraById(String userId, String tableName, String column) {
        String query = String.format(QUERY, tableName, userId);
        return new StrategyCassandraQueryStringResultCommand(session, query, column).execute();
    }

    private List<String> parseList(String resString) {
        StringBuilder sb = new StringBuilder(resString);
        sb.deleteCharAt(0);
        sb.deleteCharAt(sb.length() - 1);
        String[] array = sb.toString().replace("\"", "").split(",");
        return new ArrayList<>(Arrays.asList(array));
    }
}
