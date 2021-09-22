package mx.j2.recommend.recall.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.UserProfile;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.data_source.UserProfileTagDataSource;
import mx.j2.recommend.hystrix.redis.ZrevRangeWithScoresStragegyCommand;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.OptionalUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author: zhongren.li
 * @date: 2020-12-05
 */
@Deprecated
public class RealTimeTagRecall extends BaseRecall<BaseDataCollection> {
    private final static String PREFIX = "publisher_of";

    @Override
    public boolean skip(BaseDataCollection data) {
        return false;
    }

    @Override
    @Trace(dispatcher = true)
    public void recall(BaseDataCollection dc) {

        UserProfileTagDataSource dataSource = MXDataSource.profileTag();
        List<UserProfile.Tag> tags = dataSource.getTags(dc);

        if (MXJudgeUtils.isEmpty(tags)) {
            return;
        }

        tags.sort((o1, o2) -> o2.score.compareTo(o1.score));
        UserProfile.Tag tag = tags.get(0);

        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        String key = String.format("%s_%s", PREFIX, tag.name);
        List<BaseDocument> documents = localCacheDataSource.getTopHotTagDocumentCache(key);

        if (MXJudgeUtils.isEmpty(documents)) {
            ZrevRangeWithScoresStragegyCommand command = new ZrevRangeWithScoresStragegyCommand(key, -1);
            Map<String, Double> map = command.execute();

            List<String> ids = new ArrayList<>(map.keySet());
            documents = MXDataSource.details().get(ids, this.getName());

            OptionalUtil.ofNullable(documents)
                    .ifPresent(documents1 -> documents1.forEach(doc -> {
                        if (map.containsKey(doc.id)) {
                            doc.scoreDocument.baseScore = Float.parseFloat(map.get(doc.id).toString());
                        }
                    }));

            documents.sort((o1, o2) -> Float.compare(o2.scoreDocument.baseScore, o1.scoreDocument.baseScore));

            localCacheDataSource.setTopHotTagDocumentCache(key, documents);
        }
        dc.realTimeRecallList.addAll(documents);
    }
}
