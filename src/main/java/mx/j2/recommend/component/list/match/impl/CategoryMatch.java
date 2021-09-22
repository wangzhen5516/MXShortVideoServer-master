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
 * @description 大类别匹配
 */
@SuppressWarnings("unused")
public class CategoryMatch extends BaseProfileTagMatch {

    /**
     * 获取对应的文档大类别标签
     */
    @Override
    public Set<String> getDocumentTags(BaseDocument document) {
        if (MXJudgeUtils.isNotEmpty(document.categories)) {
            return new HashSet<>(document.categories);
        }
        return new HashSet<>();
    }
}
