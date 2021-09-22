package mx.j2.recommend.component.list.match.base;

import mx.j2.recommend.data_model.UserProfileTag;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.Set;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/7/12 下午4:36
 * @description 个性化标签匹配基类
 */
public abstract class BaseProfileTagMatch extends BaseProfileMatch<UserProfileTag> {

    @Override
    public boolean matches(UserProfileTag tag, BaseDocument document) {
        Set<String> profileTags = tag.toNameSet();
        Set<String> docTags = getDocumentTags(document);

        docTags.retainAll(profileTags);

        return MXJudgeUtils.isNotEmpty(docTags);
    }

    /**
     * 获取文档标签
     */
    public abstract Set<String> getDocumentTags(BaseDocument document);
}
