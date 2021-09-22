package mx.j2.recommend.data_source;

import com.alibaba.fastjson.JSONObject;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.UserProfile;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.hystrix.cassandra.StrategyCassandraQueryStringResultCommand;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * 用户个性化标签数据 ml_tag 数据源
 */
public class UserProfileTagDataSource extends BaseDataSource {
    private final static Logger logger = LogManager.getLogger(UserProfileTagDataSource.class);
    private final static String KEYSPACE = DefineTool.UserProfile.Tags.KEY_SPACE;
    private final static String TABLE = DefineTool.UserProfile.Tags.Table.NAME;
    private final static String QUERY = DefineTool.DB.SQL.QUERY_BY_ID_FORMAT;
    private final static String COLUMN = DefineTool.UserProfile.Tags.Table.COLUMN_TAG;

    private static CqlSession session;

    private static CqlSession longTermSession;
    private final static String LONG_KEYSPACE = DefineTool.UserProfile.Tags.KEY_SPACE;
    private final static String LONG_TABLE = "up_ml_tag_60d_v1";
    private final static String LONG_COLUMN = "tag_profile";
    private final static String LONG_QUERY = "select * from %s where uuid='%s';";

    public UserProfileTagDataSource() {
        init();
    }

    private void init() {
        try {
            session = CqlSession.builder()
                    .addContactEndPoint(new DefaultEndPoint(new InetSocketAddress(
                            Conf.getStrategyCassandraHostUrl(), Conf.getStrategyCassandraHostPort())))
                    .withLocalDatacenter(Conf.getCassandraDc())
                    .withKeyspace(KEYSPACE)
                    .build();

            longTermSession = CqlSession.builder()
                    .addContactEndPoint(new DefaultEndPoint(new InetSocketAddress(
                            Conf.getStrategyTagCassandraHost(), Conf.getStrategyTagCassandraPort())))
                    .withLocalDatacenter(Conf.getStrategyTagCassandraDc())
                    .withKeyspace(LONG_KEYSPACE)
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
    public List<UserProfile.Tag> getTags(BaseDataCollection baseDC) {
        // 如果当前确定有可用的标签，直接返回缓存的结果
        if (baseDC.client.user.profile.hasTags()) {
            return new ArrayList<>(baseDC.client.user.profile.getTags());
        }

        // 如果当前没有可用的标签，先判断是否从远端拉取过
        if (baseDC.client.user.profile.isTagSet()) {
            // 当前已经拉取过了，说明该用户此次请求没有拉到标签，认为没有，再问也没有，爱咋咋地
            return null;
        }

        if (MXStringUtils.isEmpty(baseDC.client.user.uuId)) {
            return null;
        }

        /*
         * 第一次拉取，且只拉一次
         */

        String query = String.format(QUERY, TABLE, baseDC.client.user.uuId);
        String tagsStr = new StrategyCassandraQueryStringResultCommand(session, query, COLUMN).execute();
        List<UserProfile.Tag> tags = parseTags(tagsStr);
        baseDC.client.user.profile.setTags(tags);

        return new ArrayList<>(baseDC.client.user.profile.getTags());
    }

    public List<UserProfile.Tag> getLongTermTags(BaseDataCollection baseDC) {
        if (baseDC.longTermUserProfile.hasTags()) {
            return new ArrayList<>(baseDC.longTermUserProfile.getTags());
        }

        if (baseDC.longTermUserProfile.isTagSet()) {
            return null;
        }

        if (MXStringUtils.isEmpty(baseDC.client.user.uuId)) {
            return null;
        }

        String query = String.format(LONG_QUERY, LONG_TABLE, baseDC.client.user.uuId);
        String tagsStr = new StrategyCassandraQueryStringResultCommand(longTermSession, query, LONG_COLUMN).execute();
        List<UserProfile.Tag> tags = parseTags(tagsStr);
        baseDC.longTermUserProfile.setTags(tags);

        return new ArrayList<>(baseDC.longTermUserProfile.getTags());
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
}
