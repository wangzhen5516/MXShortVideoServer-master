package mx.j2.recommend.recall.impl;

import mx.j2.recommend.data_model.UserProfile;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.document.ShortDocument;
import mx.j2.recommend.data_source.UserProfileTagDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.MXJudgeUtils;
import org.springframework.beans.BeanUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 基于个性化标签的召回
 *
 * 1 从已缓存好的数据源召回，而不是临时从 CA 中召回
 * 2 只召回 > 0 且分数最高的 3 个标签池子
 *
 * @see mx.j2.recommend.data_source.TagTopVideoDataSource
 * @see mx.j2.recommend.mixer.impl.UserProfileTagTopMixer
 */
@SuppressWarnings("unused")
@Deprecated
public class UserProfileTagTopRecall extends BaseRecall<BaseDataCollection> {
    int TAG_NUM = 1;// 只需要前 1 个标签
    String REDIS_PREFIX = "tophot_ml_tag_";

    @Override
    public boolean skip(BaseDataCollection data) {
        return false;
    }

    @Override
    public void recall(BaseDataCollection baseDc) {
        StringBuilder log = new StringBuilder();

        // 借助标签数据源工具类去获取标签
        UserProfileTagDataSource dataSource = MXDataSource.profileTag();
        List<UserProfile.Tag> tags = dataSource.getTags(baseDc);

        // 记录所有的 tag 数量
        log.append("Tags:").append(tags != null ? tags.size() : 0);

        // 木有标签，啥也不干
        if (MXJudgeUtils.isEmpty(tags)) {
            return;
        }

        /*
         * 过滤、排序并截断，只保留最多 1 个大于 0 且分数最高的标签
         */
        try {
            tags = tags.stream().filter(tag -> tag.score > 0).collect(Collectors.toList());
            tags.sort((o1, o2) -> o2.score.compareTo(o1.score));
            tags = tags.subList(0, Math.min(TAG_NUM, tags.size()));

            // 记录所有的 tag 数量
            log.append("/").append(tags.size());
        } catch (Exception e) {
            e.printStackTrace();
        }

        log.append(", Used:{");

        // 为 tag 拉数据
        for (UserProfile.Tag tagIt : tags) {
            List<BaseDocument> list = MXDataSource.tagTop().getVideosByTag(REDIS_PREFIX, tagIt.name, getName());
            if (MXJudgeUtils.isNotEmpty(list)) {
                List<BaseDocument> copyList = new ArrayList<>(list.size());
                deepClone(list, copyList);
                baseDc.userProfileTagMap.put(tagIt, copyList);
            }

            // 记录 tag 的召回数量
            log.append(tagIt.name).append(":").append(list != null ? list.size() : 0);
        }

        log.append("}");

        // 为此召回组件记录额外的日志
        baseDc.logComponentExtra.put(getName(), log.toString());
        doSomethingAfterRecall(baseDc);
    }

    protected void doSomethingAfterRecall(BaseDataCollection baseDc) {
    }

    private void deepClone(List<BaseDocument> source, List<BaseDocument> target) {
        source.forEach(doc -> {
            BaseDocument bdoc = new ShortDocument();
            if (null != bdoc) {
                BeanUtils.copyProperties(doc, bdoc);
                if (null != doc) {
                    bdoc.recallName = doc.recallName;
                }
                target.add(bdoc);
            }
        });
    }
}
