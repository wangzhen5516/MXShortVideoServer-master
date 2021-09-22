package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.UserProfile;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;

import java.util.Set;

public class ProfileTagHindiFilter extends BaseFilter {
    private final String KEEP_TELUGU = "language_Telugu";
    private final String KEEP_KANNADA = "language_Kannada";
    private final String KEEP_TAMIL = "language_Tamil";
    private final String FILTER_HINDI = "language_Hindi";
    private final String LONG_TERM_TABLE_NAME = "up_ml_tag_60d_v1";
    private final String HINDI_LANGUAGE = "Hindi";

    @Override
    public boolean prepare(BaseDataCollection dc) {
        boolean hasLongTermTag = dc.client.user.profile.profileTags.pull(UserProfile.Tag.TypeEnum.LONGTERMTAG,
                dc.client.user.uuId, LONG_TERM_TABLE_NAME);
        return hasLongTermTag && (Math.abs(dc.client.user.uuId.hashCode() + 1) % 2 != 0);
    }

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {
        Set<String> profileTags = dc.client.user.profile.profileTags.longTermTag.toNameSet();
        if (profileTags.contains(FILTER_HINDI)) {
            return false;
        }

        if ((profileTags.contains(KEEP_TELUGU) || profileTags.contains(KEEP_KANNADA) || profileTags.contains(KEEP_TAMIL)) && !profileTags.contains(FILTER_HINDI)) {
            // 过滤有且只有Hindi语言的视频
            return doc.languageIdList.size() == 1 && doc.languageIdList.contains(HINDI_LANGUAGE);
        }
        return false;
    }
}
