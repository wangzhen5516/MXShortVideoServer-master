package mx.j2.recommend.recall.impl;

import mx.j2.recommend.data_model.UserProfile;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.document.ShortDocument;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.MXJudgeUtils;
import org.springframework.beans.BeanUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * 改造召回方式为同 tagtop 相同
 */
public class StrategyTagTopHotV6Recall extends StrategyTagPoolRecall {

    String REDIS_PREFIX = "tophot_ml_tag_v6_";

    @Override
    protected void setTagTable(BaseDataCollection dc) {
        dc.tagTableName = "up_human_tag_7d_v2";
    }

    @Override
    public void doRecall(BaseDataCollection dc, List<String> userTags) {
        // 为 tag 拉数据
        for (String tagIt : userTags) {
            List<BaseDocument> list = MXDataSource.tagTop().getVideosByTag(REDIS_PREFIX, tagIt, getName());
            if (MXJudgeUtils.isNotEmpty(list)) {
                List<BaseDocument> copyList = new ArrayList<>(list.size());
                deepClone(list, copyList);

                // 这块敢给 0 分的原因是 A 组混入时没用到分数了
                dc.userProfileTagMap.put(new UserProfile.Tag(tagIt, 0), copyList);
            }
        }
    }

    private void deepClone(List<BaseDocument> source, List<BaseDocument> target) {
        source.forEach(doc -> {
            BaseDocument bdoc = new ShortDocument();
            BeanUtils.copyProperties(doc, bdoc);
            bdoc.recallName = doc.recallName;
            target.add(bdoc);
        });
    }
}
