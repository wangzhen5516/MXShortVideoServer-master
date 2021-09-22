package mx.j2.recommend.mixer.impl;

import mx.j2.recommend.data_model.UserProfile;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * 混入个性化标签召回的数据
 * <p>
 * 这里只混入喜欢的数据，即 tag_score > 0 的
 *
 * @see mx.j2.recommend.recall.impl.UserProfileTagTopRecall
 * @see mx.j2.recommend.data_source.TagTopVideoDataSource
 */
@SuppressWarnings("unused")
public class UserProfileTagTopMixer extends BaseMixer<BaseDataCollection> {
    private static final int MIX_NUM_LEVEL_1 = 1;// 一档混入数
    private static final int MIX_NUM_LEVEL_2 = 2;// 二档混入数

    // 各档位混入数量数组
    private static final int[] MIX_NUM = {MIX_NUM_LEVEL_1, MIX_NUM_LEVEL_2};

    @Override
    public boolean skip(BaseDataCollection data) {
        if (new Random().nextDouble() > 0.1) {
            return true;
        }

        return MXJudgeUtils.isEmpty(data.userProfileTagMap) || isPureNewUser(data);
    }

    @Override
    public void mix(BaseDataCollection dc) {
        List<BaseDocument> addList = new ArrayList<>();
        float tagScoreIt;
        int mixNumIndexIt;
        int mixNumIt;
        List<BaseDocument> docsIt;

        /*
         * 目前的需求是最多混入 6 个，在 recall 阶段已经保证最多 3 个 tag，
         * 每个 tag 这里又最多混入 2 个，所以不会超过 6 个，不需要额外判断
         *
         * 2020/12/1  最多混入1个
         */

        for (Map.Entry<UserProfile.Tag, List<BaseDocument>> entry : dc.userProfileTagMap.entrySet()) {
            docsIt = entry.getValue();

            if (MXJudgeUtils.isNotEmpty(docsIt)) {
                 /*tagScoreIt = entry.getKey().score;
                 mixNumIndexIt = getNumIndex(tagScoreIt);
                 mixNumIt = MIX_NUM[mixNumIndexIt];

                 addList.addAll(docsIt.subList(0, Math.min(mixNumIt, docsIt.size())));*/

                addList.add(docsIt.get(0));
            }
        }

        addDocsToMixDocument(dc, addList);
    }

    /**
     * 根据分数获取档位混入数组索引
     * <p>
     * 一档：0 < score < 0.5
     * 二档：0.5 <= score <= 1
     *
     * @param score [-1,1]
     *              到这里已经滤去了[-1,0] 之间的分数
     * @return 混入数组索引
     */
    private int getNumIndex(float score) {
        if (score > 0 && score < 0.5) {
            return 0;
        } else {
            return 1;
        }
    }
}
