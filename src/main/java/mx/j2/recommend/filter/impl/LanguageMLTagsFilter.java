package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.data_collection.FeedDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.document.ShortDocument;
import mx.j2.recommend.thrift.Request;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.ArrayList;

/**
 * 个性化标签里如果有语言标签，则必须在 languageList 里，否则过滤掉
 */
@SuppressWarnings("unused")
public class LanguageMLTagsFilter extends BaseFilter {

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection baseDc) {
        if (null == doc) {
            return true;
        }

        if (baseDc == null || baseDc.req == null || MXJudgeUtils.isEmpty(baseDc.req.languageList)
                || MXJudgeUtils.isEmpty(doc.mlTags)) {
            return false;
        }

        // 标签列表里是否存在语言标签
        boolean hasLanguageTag = false;

        for (String tag : doc.mlTags) {
            // 为标签提取出语言
            DefineTool.Language language = DefineTool.UserProfile.Tags.unwrapLanguage(tag);

            if (!DefineTool.Language.NoLanguage.equals(language)) {// 是语言标签
                hasLanguageTag = true;// 标记有语言标签

                if (baseDc.req.languageList.contains(language.id)) {// 在请求的语言中
                    return false;// 不过滤
                }
            }
        }

        return hasLanguageTag;
    }

    public static void main(String[] args) {
        LanguageMLTagsFilter filter = new LanguageMLTagsFilter();
        ShortDocument document = new ShortDocument();
        BaseDataCollection dc = new FeedDataCollection();
        dc.req = new Request();
        dc.req.languageList = new ArrayList<>();
        dc.req.languageList.add("te");
        dc.req.languageList.add("ta");
        document.mlTags.add("language_Telugu");
        document.mlTags.add("language_Tamil2");
        filter.isFilted(document, dc);
    }
}
