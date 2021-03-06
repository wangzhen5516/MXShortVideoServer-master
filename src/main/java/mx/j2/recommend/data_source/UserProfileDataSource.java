package mx.j2.recommend.data_source;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.hystrix.cassandra.StrategyCassandraQueryStringResultCommand;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.*;

import static mx.j2.recommend.util.BaseMagicValueEnum.NewUserWatchNumber;


public class UserProfileDataSource extends BaseDataSource {
    private final static Logger logger = LogManager.getLogger(UserProfileDataSource.class);
    private final static String KEYSPACE = "mx_takatak_userprofile";
    private final static String userProfileTable = "taka_userprofile";
    private final static String getUserProfileByUserIdQuery = "select * from %s where id='%s';";

    private static CqlSession session;

    public UserProfileDataSource() {
        init();
    }

    private void init() {
        try {
            session = CqlSession.builder()
                    .addContactEndPoint(new DefaultEndPoint(new InetSocketAddress(
                            Conf.getStrategyCassandraHostUrl(), Conf.getStrategyCassandraHostPort())))
                    .withLocalDatacenter(Conf.getStrategyCassandraDc())
                    .withKeyspace(KEYSPACE)
                    .build();
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
        return new StrategyCassandraQueryStringResultCommand(session, query, "pro").execute();
    }

    @Trace(dispatcher = true)
    public String getUserGenderInfo(String userProfile, String targetUserId) {
        String gender = "unknown";
        if (MXStringUtils.isNotEmpty(userProfile) && targetUserId != null) {
            JSONObject userprofileObj = JSON.parseObject(userProfile);
            if (null != userprofileObj && userprofileObj.containsKey("user_ids")) {
                JSONArray userIds = userprofileObj.getJSONArray("user_ids");
                if (MXJudgeUtils.isNotEmpty(userIds)) {
                    //???????????????10??????????????????
                    for (int i = 0; i < Math.min(10, userIds.size()); i++) {
                        JSONObject obj = (JSONObject) userIds.get(i);
                        if (null != obj && obj.containsKey("userid") && obj.getString("userid").equals(targetUserId)) {
                            if (obj.containsKey("gender")) {
                                String res = obj.getString("gender");
                                if (MXStringUtils.isNotEmpty(res.trim())) {
                                    gender = res;
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
        return gender;
    }

    //??????prefferedPublisherIdList
    @Trace(dispatcher = true)
    public List<String> getUserPreferredPublisherList(String userProfile) {
        List<String> publisherList = new ArrayList<>();
        if (MXStringUtils.isNotEmpty(userProfile)) {
            JSONObject userprofileObj = JSON.parseObject(userProfile);
            if (null != userprofileObj && userprofileObj.containsKey("publisher_id")) {
                JSONArray publisherJsonList = userprofileObj.getJSONArray("publisher_id");
                if (MXJudgeUtils.isNotEmpty(publisherJsonList)) {
                    for (int i = 0; i < publisherJsonList.size(); i++) {
                        JSONArray publisherTuple = publisherJsonList.getJSONArray(i);
                        if (MXJudgeUtils.isNotEmpty(publisherTuple) && publisherTuple.size() == 2) {
                            String publisherId = String.valueOf(publisherTuple.get(0));
                            if (MXStringUtils.isNotEmpty(publisherId)) {
                                publisherList.add(publisherId);
                            }
                        }
                    }
                }
            }
        }
        return publisherList;
    }

    @Trace(dispatcher = true)
    public List<String> getUserProfileByField(String userProfile, String field) {
        if (MXStringUtils.isEmpty(userProfile) || MXStringUtils.isEmpty(field)) {
            return null;
        }

        JSONObject object = JSONObject.parseObject(userProfile);
        if (!Optional.ofNullable(object).isPresent()) {
            return null;
        }

        if (!object.containsKey(field)) {
            return null;
        }

        JSONArray array = JSONArray.parseArray(object.getString(field));
        if (MXJudgeUtils.isEmpty(array)) {
            return null;
        }

        List<String> list = new ArrayList<>();
        array.forEach(e -> {
            JSONArray elementArray = (JSONArray) e;
            if (MXJudgeUtils.isEmpty(elementArray)) {
                return;
            }

            list.add(elementArray.getString(0));
        });
        return list;
    }

    /**
     * ??????????????????????????????
     * @param dc
     * @return
     */
    public static boolean isPureNewUser(BaseDataCollection dc) {
        return NewUserWatchNumber > dc.userHistorySize;
    }

    /**
     * ??????????????????????????????
     * @param dc
     * @return
     */
    public static boolean isUserOlderThan(BaseDataCollection dc, int threshold) {
        return threshold < dc.userHistorySize;
    }

    /**
     * ??????????????????????????????
     * @param dc
     * @return
     */
    public static boolean isUserNewerThan(BaseDataCollection dc, int threshold) {
        return threshold >= dc.userHistorySize;
    }

    /**
     * ??????????????????????????????????????????
     * @param dc
     * @return
     */
    public boolean isUserHistoryExceedValue(BaseDataCollection dc, int limit) {
        if (limit >= 0) {
            return limit < dc.userHistorySize;
        }
        return true;
    }

    /**
     * ??????json?????????tags?????????Map
     */
    public static Map<String, Double> getUserTagsScoreFromResult(String result) {
        return getMapFromArrayInResult(result, "tags");
    }

    /**
     * ??????json?????????desc_tag?????????Map
     */
    public static Map<String, Double> getUserDescTagScoreFromResult(String result) {
        return getMapFromArrayInResult(result, "desc_tag");
    }

    /**
     * ??????json?????????publisher_id?????????Map
     */
    public static Map<String, Double> getUserPublisherIdScoreFromResult(String result) {
        return getMapFromArrayInResult(result, "publisher_id");
    }

    public static Map<String, Double> getMapFromArrayInResult(String result, String parameter) {
        if (MXStringUtils.isEmpty(result)) {
            return Collections.emptyMap();
        }

        JSONObject userprofileObj = JSON.parseObject(result);
        if (null == userprofileObj || !userprofileObj.containsKey(parameter)) {
            return Collections.emptyMap();
        }

        JSONArray publisherJsonList = userprofileObj.getJSONArray(parameter);
        if (MXJudgeUtils.isEmpty(publisherJsonList)) {
            return Collections.emptyMap();
        }

        Map<String, Double> scoreMap = new HashMap<>();
        for (int i = 0; i < publisherJsonList.size(); i++) {
            JSONArray publisherTuple = publisherJsonList.getJSONArray(i);
            if (MXJudgeUtils.isNotEmpty(publisherTuple) && publisherTuple.size() == 2) {
                String publisherId = String.valueOf(publisherTuple.get(0));
                Double score = publisherTuple.getDouble(1);
                scoreMap.put(publisherId, score);
            }
        }
        return scoreMap;
    }

    /**
     * ?????????????????????????????????Document??????????????????,??????????????????
     * @param uuid ??????UUID
     * @param docs Document???List
     * @return ??????????????????List
     */
    @Trace(dispatcher = true)
    public List<String> getPrediction(String uuid, List<BaseDocument> docs, String userProfileResult) {
        if(userProfileResult == null) {
            userProfileResult = getUserProfileByUuId(uuid);
        }
        Map<String, Double> videoScore = getVideoScore(uuid);
        Map<String, Double> tagsScore = getUserTagsScoreFromResult(userProfileResult);
        Map<String, Double> descTagScore = getUserDescTagScoreFromResult(userProfileResult);
        Map<String, Double> publisherIdScore = getUserPublisherIdScoreFromResult(userProfileResult);

        List<String> ret = new ArrayList<>();
        for(BaseDocument d : docs) {
            String userProfilePredictFormat = "0.0,%.4f,%.4f,%.4f,%.4f";
            String predictString = String.format(userProfilePredictFormat,
                    videoScore.getOrDefault(d.id, 0.0),
                    calculateCos(tagsScore, d.tagString),
                    calculateCos(descTagScore, d.descTag),
                    publisherIdScore.getOrDefault(d.publisher_id, 0.0)
                    );
            ret.add(predictString);
        }
        return ret;
    }

    public static String getUserProfile(String userProfileResult) {
        String userStr = "0,0,0,,0";
        if(MXStringUtils.isBlank(userProfileResult)) {
            return userStr;
        }

        JSONObject userprofileObj = JSON.parseObject(userProfileResult);
        if (null == userprofileObj || !userprofileObj.containsKey("user_ids")) {
            return userStr;
        }

        JSONArray publisherJsonList = userprofileObj.getJSONArray("user_ids");
        if (MXJudgeUtils.isEmpty(publisherJsonList)) {
            return userStr;
        }

        int userProfileBl = 0;
        int userProfileFl = 0;
        int userProfileFn = 0;
        String userProfileGender="";
        int userProfileAge = 0;

        JSONObject o = publisherJsonList.getJSONObject(0);
        try{
            userProfileBl = o.getIntValue("bl");
            userProfileFl =o.getIntValue("fl");
            userProfileFn =o.getIntValue("fn");
            userProfileGender =o.getString("gender");
            userProfileAge =o.getIntValue("age");

        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Exception happen at trans UserProfile bl, fl, etc "+o.toJSONString());
        }

        userStr = String.format("%d,%d,%d,%s,%d",
                userProfileBl,
                userProfileFl,
                userProfileFn,
                userProfileGender,
                userProfileAge);
        return userStr;
    }

    private Map<String, Double> getVideoScore(String uuid) {
        String result = MXDataSource.strategyCA().getStrategyOutPutFromCassandraById(uuid, "personal_reco_act_pub_cf");
        if (MXStringUtils.isEmpty(result)) {
            return Collections.EMPTY_MAP;
        }
        JSONArray jsonArray = JSON.parseArray(result);
        Map<String, Double> scoreMap = new HashMap<>();
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject jsonObject = (JSONObject) jsonArray.get(i);
            if (jsonObject != null && MXStringUtils.isNotEmpty(jsonObject.getString("id")) && jsonObject.getDouble("score") != null) {
                scoreMap.put(jsonObject.getString("id"), jsonObject.getDouble("score"));
            }
        }
        return scoreMap;
    }

    /**
     * ??????Map???Set???cos??????????????????Set??????????????????score???1.0
     * @param scoreMap
     * @param tags
     * @return
     */
    public static double calculateCos(Map<String, Double> scoreMap, Set<String> tags) {
        // ????????????
        Map<String, Double> aMap = scoreMap;
        Map<String, Double> bMap = new HashMap<>();
        for (String b1 : tags) {
            bMap.put(b1, bMap.getOrDefault(b1, 0.0) + 1);
        }

        // ?????????
        Set<String> union = new HashSet<>(aMap.keySet());
        union.addAll(tags);
        double[] aVec = new double[union.size()];
        double[] bVec = new double[union.size()];
        List<String> collect = new ArrayList<>(union);
        for (int i = 0; i < collect.size(); i++) {
            aVec[i] = aMap.getOrDefault(collect.get(i), 0.0);
            bVec[i] = bMap.getOrDefault(collect.get(i), 0.0);
        }

        // ????????????????????????
        int p1 = 0;
        for (int i = 0; i < aVec.length; i++) {
            p1 += (aVec[i] * bVec[i]);
        }

        double p2 = 0f;
        for (double i : aVec) {
            p2 += (i * i);
        }
        p2 = Math.sqrt(p2);

        double p3 = 0f;
        for (double i : bVec) {
            p3 += (i * i);
        }
        p3 =  Math.sqrt(p3);

        return p1 / (p2 * p3);
    }
    //??????BasePredictor???????????????
    public int getUserFollowPubPridictNum(BaseDataCollection dc){
        if(dc.userFollowPublishListSize <= 0){
            return 0;
        }
        if (0 < dc.userFollowPublishListSize && dc.userFollowPublishListSize < 200) {
            return 10;
        } else if (200 <= dc.userFollowPublishListSize && dc.userFollowPublishListSize < 1000) {
            return 20;
        }else{
            return 30;
        }
    }
}
