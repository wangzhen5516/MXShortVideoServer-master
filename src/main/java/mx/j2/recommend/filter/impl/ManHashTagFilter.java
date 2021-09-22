package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.UserProfileDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.MXStringUtils;

import java.util.ArrayList;
import java.util.List;

import static mx.j2.recommend.util.DefineTool.GenderEnum.MALE;

public class ManHashTagFilter extends BaseFilter {
    List<String> filterList = new ArrayList<String>() {
        {
            add("nail");
            add("makeup");
            add("hair");
            add("mehend");
            add("lips");
        }
    };

    @Override
    public boolean prepare(BaseDataCollection dc) {
        UserProfileDataSource userProfileDataSource = MXDataSource.profile();
        String userProfile = userProfileDataSource.getUserProfileByUuId(dc.client.user.uuId);
        String gender = userProfileDataSource.getUserGenderInfo(userProfile, dc.client.user.userId);
        if (MXStringUtils.isNotEmpty(gender)) {
            dc.client.user.profile.gender = gender;
        }

        return true;
    }

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {
        if (doc == null) {
            return true;
        }

        if (MXStringUtils.isEmpty(doc.description)) {
            return false;
        }

        if (dc.client.user.profile.gender.equals(MALE.getName())) {
            for (String tag : filterList) {
                if (doc.description.matches("[\\s\\S]*#[\\w]*(?i)" + tag + "[\\s\\S]*")) {
                    return true;
                }
            }
        }
        return false;
    }

    public static void main(String[] args) {
        List<String> filterList = new ArrayList<String>() {
            {
                add("human_tag_in_aaa");
                add("fdsaf_human_tag_in_bbb");
                add("hair");
                add("mehend");
                add("lips");
            }
        };
        String desc = "aaa #aaamakeupbbb #fsafs #hjghj dsf";
        for (String tag : filterList) {
            if (tag.matches("human_tag_in_[\\w]*")) {
                System.out.println(tag);
            }
            /*if (desc.matches("[\\s\\S]*#[\\w]*(?i)" + tag + "[\\s\\S]*")) {
                System.out.println(tag);
            }*/
        }
        //[\\s\\S]*#[\\w]*(?i)" + tag + "[\\w]*
    }
}
