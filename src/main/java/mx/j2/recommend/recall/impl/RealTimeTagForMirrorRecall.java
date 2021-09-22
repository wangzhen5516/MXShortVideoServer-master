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
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @Author: xiaoling.zhu
 * @Date: 2021-02-20
 */

public class RealTimeTagForMirrorRecall extends BaseRecall<BaseDataCollection> {
    int TAG_NUM = 1;
    String REDIS_PREFIX = "tophot_ml_tag_v6_";

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
         * 过滤、不排序，但是截断前三个，打混随机
         */
        try {
            tags = tags.stream().filter(tag -> tag.score > 3).collect(Collectors.toList());
            processTags(tags);
            //打混tags
            Collections.shuffle(tags);
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
    }

    private void deepClone(List<BaseDocument> source, List<BaseDocument> target) {
        source.forEach(doc -> {
            BaseDocument bdoc = new ShortDocument();
            if (null != bdoc) {
                BeanUtils.copyProperties(doc, bdoc);
                if (null != doc) {
                    bdoc.recallName = this.getName();
                }
                target.add(bdoc);
            }
        });
    }

    /**
     * 标签处理方法，子类覆盖使用
     */
    protected void processTags(List<UserProfile.Tag> tags) {
        return;
    }
}
