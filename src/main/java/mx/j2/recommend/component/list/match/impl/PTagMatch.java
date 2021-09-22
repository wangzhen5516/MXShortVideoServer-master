package mx.j2.recommend.component.list.match.impl;

import mx.j2.recommend.component.list.match.base.BaseProfileTagMatch;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.HashSet;
import java.util.Set;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/7/12 下午4:36
 * @description 主标签匹配
 */
@SuppressWarnings("unused")
public class PTagMatch extends BaseProfileTagMatch {

    /**
     * 获取对应的文档标签
     */
    @Override
    public Set<String> getDocumentTags(BaseDocument document) {
        Set<String> tags = new HashSet<>();

        if (MXJudgeUtils.isNotEmpty(document.primaryTags)) {
            tags.addAll(document.primaryTags.toJavaList(String.class));
        }

        if (MXJudgeUtils.isNotEmpty(document.secondaryTags)) {
            tags.addAll(document.secondaryTags.toJavaList(String.class));
        }

        return tags;
    }
}
