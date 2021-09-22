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
 * @author liangjie.feng
 * @version 1.0
 * @date 2021/3/18 下午2:11
 * @description
 */
public class UserProfileTagTopRandom5V6Score30PrefixFilterRecall extends UserProfileTagTopRandom1V6PrefixFilterRecall {
    public UserProfileTagTopRandom5V6Score30PrefixFilterRecall() {
        TAG_NUM = 5;
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
            tags = tags.stream().filter(tag -> tag.score > 30).collect(Collectors.toList());
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
                    bdoc.recallName = doc.recallName;
                }
                target.add(bdoc);
            }
        });
    }
}
