package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.hystrix.GetStringFromPrivateAccountRedisCommand;
import mx.j2.recommend.hystrix.redis.ZrevRangePvCommand;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;
import org.apache.commons.collections.CollectionUtils;

import java.util.*;

/**
 * @author zhongren.li
 * @date 2021-04-28 17:17
 */
public class LastActionVideoFromRedisRecall extends BaseRecall<BaseDataCollection> {

    protected static final int RECALL_SIZE = 200;

    private static final String REDIS_KEY_FORMAT = "item_reco_cf_03-%s";

    private static final int RELATE_VIDEOS_NUMBER = 5;

    private static final int MAX_FIND_RELATED_VIDEOS_NUMBERS = 8;

    private static final List<Integer> SUBSECTION = new ArrayList<Integer>(){
        {
            add(50);add(10);add(5);
        }
    };

    @Override
    public boolean skip(BaseDataCollection dc) {
        if (dc.req == null || dc.req.userInfo == null) {
            return true;
        }
        String uuId = dc.req.userInfo.getUuid();
        if (MXStringUtils.isBlank(uuId)) {
            return true;
        }
        return false;
    }

    public String getKey(BaseDataCollection dc) {
        return String.format("%s:latest_action_video_v1", dc.req.userInfo.getUuid());
    }

    public int getLength() {
        return RECALL_SIZE;
    }

    @Override
    @Trace(dispatcher = true)
    public void recall(BaseDataCollection dc) {
        ZrevRangePvCommand pvCommand = new ZrevRangePvCommand(getKey(dc), 0, getLength());

        //TODO
        long startTime = System.nanoTime();
        List<String> videoIds = pvCommand.execute();
        dc.appendToTimeRecord(System.nanoTime()-startTime,this.getName()+"_PvCommand_execute");
        if (MXJudgeUtils.isEmpty(videoIds)) {
            return;
        }

        List<String> selected = new ArrayList<>();
        selectVideos(videoIds, selected);

        List<String> results = new ArrayList<>();
        getRelateVideosNew(selected, results);
//        getRelateVideos(videoIds, results);
        if (CollectionUtils.isEmpty(results)) {
            return;
        }

        //TODO
        startTime = System.nanoTime();
        List<BaseDocument> docLists = MXDataSource.details().get(results, this.getName());
        dc.appendToTimeRecord(System.nanoTime()-startTime,this.getName()+"_Detail");

        if (MXJudgeUtils.isEmpty(docLists)) {
            return;
        }

        /*按照heat_score2排序*/
        docLists.sort(Comparator.comparing(BaseDocument::getHeatScore2).reversed());
        dc.relatedList.addAll(docLists);
        dc.syncSearchResultSizeMap.put(this.getName(), docLists.size());
        dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.REDIS.getName());
    }

    private void selectVideos(List<String> videoIds, List<String> selected) {
        Random  random = new Random();
        int start = 0;
        int end = videoIds.size() - 1;
        for (int limit : SUBSECTION) {
            if (end <= limit) {
                continue;
            }
            if (limit == SUBSECTION.get(SUBSECTION.size() - 1)) {
                if (random.nextDouble() < 0.8) {
                    for (int i = start; i < end; i++) {
                        if (selected.size() >= RELATE_VIDEOS_NUMBER) {
                            return;
                        }
                        selected.add(videoIds.get(i));
                    }
                } else {
                    List<String> temp = new ArrayList<>(videoIds.subList(start+limit, end));
                    Collections.shuffle(temp);
                    for (String s : temp) {
                        if (selected.size() >= RELATE_VIDEOS_NUMBER) {
                            return;
                        }
                        selected.add(s);
                    }
                }
            } else {
                if (random.nextDouble() < 0.5) {
                    end = start + limit;
                } else {
                    start = start + limit;
                }
            }
        }
    }

    private void getRelateVideosNew(List<String> selected, List<String> results) {
        Set<String> idSet = new HashSet<>();
        for (String vId : selected) {
            List<String> temp = getRelatedList(String.format(REDIS_KEY_FORMAT, vId));
            if (CollectionUtils.isEmpty(temp)) {
                continue;
            }

            idSet.addAll(temp);
        }

        if (idSet.size() > 200) {
            List<String> temp = new ArrayList<>(idSet);
            Collections.shuffle(temp);
            results.addAll(temp.subList(0, 200));
        } else {
            results.addAll(idSet);
        }
    }

    private void getRelateVideos(List<String> videoIds, List<String> results) {
        int count = 0;
        int sum = 0;
        Set<String> idSet = new HashSet<>();
        Collections.shuffle(videoIds);
        for (String vId : videoIds) {
            if (sum++ >= MAX_FIND_RELATED_VIDEOS_NUMBERS) {
                break;
            }
            if (count >= RELATE_VIDEOS_NUMBER) {
                break;
            }
            List<String> temp = getRelatedList(String.format(REDIS_KEY_FORMAT, vId));
            if (CollectionUtils.isEmpty(temp)) {
                continue;
            }

            idSet.addAll(temp);
            count++;
        }

        if (idSet.size() > 200) {
            List<String> temp = new ArrayList<>(idSet);
            Collections.shuffle(temp);
            results.addAll(temp.subList(0, 200));
        } else {
            results.addAll(idSet);
        }
    }

    private List<String> getRelatedList(String key) {
        GetStringFromPrivateAccountRedisCommand command = new GetStringFromPrivateAccountRedisCommand(key);
        String jsonString = command.execute();
        if (MXStringUtils.isEmpty(jsonString)) {
            return null;
        }
        return loadJsonString(jsonString);
    }

    private List<String> loadJsonString(String jsonString) {
        JSONArray jsonArray = JSONArray.parseArray(jsonString);
        if (MXJudgeUtils.isEmpty(jsonArray)) {
            return null;
        }

        List<String> relatedIds = new ArrayList<>();
        for (Object o : jsonArray) {
            JSONObject obj = (JSONObject) o;
            if (obj.containsKey("id")) {
                relatedIds.add(obj.getString("id"));
            }
        }
        return relatedIds;
    }
}