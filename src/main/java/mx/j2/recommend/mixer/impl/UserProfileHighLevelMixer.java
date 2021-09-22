package mx.j2.recommend.mixer.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.UserProfile;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.UserProfileTagDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.pool_conf.PoolConf;
import mx.j2.recommend.util.BaseMagicValueEnum;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author ：zhongrenli
 * @date ：Created in 4:52 下午 2021/01/15
 */
public class UserProfileHighLevelMixer extends BaseMixer<BaseDataCollection> {

    private static final Set<String> POSITIVE_TAGS = new HashSet<String>(){
        {
            add("funny");add("food");add("comedy");add("dog");
            add("baby");add("cat");add("makeup");add("bts");
        }
    };

    @Override
    public boolean skip(BaseDataCollection dc) {
        if (MXJudgeUtils.isEmpty(dc.poolToDocumentListMap)) {
            System.out.println("poolToDocumentListMap is Empty!");
            return true;
        }

        if (null != dc.client.user.profile  && MXJudgeUtils.isNotEmpty(dc.client.user.profile.getLowScoreMap())) {
            return false;
        }

        if (resultIsEnough(dc)) {
            return true;
        }

        UserProfileTagDataSource dataSource = MXDataSource.profileTag();
        List<UserProfile.Tag> tags = dataSource.getTags(dc);
        if (MXJudgeUtils.isEmpty(tags)) {
            return false;
        }
        tags = tags.stream().filter(tag -> tag.score < 0).collect(Collectors.toList());

        Map<String, Float> lowScoreMap = new HashMap<>();
        for (UserProfile.Tag tag : tags) {
            lowScoreMap.put(tag.name, tag.score);
        }
        dc.client.user.profile.setLowScoreMap(lowScoreMap);

        return false;
    }

    @Override
    @Trace(dispatcher = true)
    public void mix(BaseDataCollection dc) {
        Map<String, PoolConf> map = new LinkedHashMap<>();
        Map<String, Map<String, PoolConf>> levelToMap =  MXDataSource.pools().all();
        for (Map<String, PoolConf> item : levelToMap.values()) {
            PoolConf pc = item.getOrDefault(dc.recommendFlow.name, item.get("base"));
            if(pc == null) {
                continue;
            }
            map.put(pc.poolIndex, pc);
        }

        List<BaseDocument> userProfileTag = new ArrayList<>();
        List<BaseDocument> normal = new ArrayList<>();
        for (Map.Entry<String, PoolConf> entry : map.entrySet()) {
            String poolIndex = entry.getKey();
            if (!dc.poolToDocumentListMap.containsKey(poolIndex)) {
                continue;
            }
            List<BaseDocument> documentList = dc.poolToDocumentListMap.get(poolIndex);
            if (MXJudgeUtils.isEmpty(documentList)) {
                continue;
            }
            PoolConf conf = entry.getValue();

            // 非高级池子跳过
            if (!conf.poolLevel.contains(DefineTool.EsPoolLevel.HIGH.getLevel())) {
                continue;
            }

            if (Double.compare(conf.percentage, 0) <= 0) {
                continue;
            }

            for (BaseDocument doc : documentList) {
                if(doc.recallName.startsWith("PoolRecall")) {
                    doc.setPoolLevel(BaseMagicValueEnum.HIGH_LEVEL);
                    doc.setPoolIndex(conf.poolIndex);
                } else if(doc.recallName.startsWith("SingleToPool11Recall")){
                    doc.setPoolLevel(BaseMagicValueEnum.HIGH_LEVEL);
                    doc.setPoolIndex(conf.poolIndex);
                } else if(doc.recallName.startsWith("TagRecall")){
                    doc.setPoolLevel(BaseMagicValueEnum.HIGH_LEVEL);
                    doc.setPoolIndex(conf.poolIndex);
                }
                normal.add(doc);

                if (null != dc.client.user.profile && MXJudgeUtils.isNotEmpty(dc.client.user.profile.getLowScoreMap())) {
                    Set<String> tempSet = new HashSet<>(dc.client.user.profile.getLowScoreMap().keySet());
                    tempSet.retainAll(POSITIVE_TAGS);
                    tempSet.retainAll(doc.mlTags);
                    if (MXJudgeUtils.isNotEmpty(tempSet)) {
                        doc.setTagScore(1);
                        userProfileTag.add(doc);
                    }
                }
            }
        }

        if (MXJudgeUtils.isNotEmpty(userProfileTag)) {
            userProfileTag.sort((o1, o2) -> Integer.compare(o2.getTagScore(), o1.getTagScore()));
            userProfileTag.sort((o1, o2) -> Integer.compare(o2.getPriority(), o1.getPriority()));
        }

        merge(dc, userProfileTag, normal);
    }

    private void merge(BaseDataCollection dc, List<BaseDocument> list1, List<BaseDocument> list2) {
        if (MXJudgeUtils.isEmpty(list1)) {
            mergeOneList(dc, list2);
        }

        if (MXJudgeUtils.isEmpty(list2)) {
            mergeOneList(dc, list1);
        }

        int index1 = 0;
        int index2 = 0;
        while(!resultIsEnough(dc)) {
            while (index1 < list1.size()) {
                boolean r = addDocToMixDocument(dc, list1.get(index1));
                if (r) {
                    index1++;
                    break;
                }
                index1++;
            }

            if (resultIsEnough(dc)) {
                break;
            }

            while (index2 < list2.size()) {
                boolean r = addDocToMixDocument(dc, list2.get(index2));
                index2++;
                if (r) {
                    break;
                }
            }

            if (index1 >= list1.size() && index2 >= list2.size()) {
                break;
            }
        }
    }

    private void mergeOneList(BaseDataCollection dc, List<BaseDocument> list) {
        if (MXJudgeUtils.isEmpty(list)) {
            return;
        }

        for (BaseDocument doc : list){
            addOneDocToMixDocument(dc, doc);

            if (resultIsEnough(dc)) {
                return;
            }
        }
    }
}
