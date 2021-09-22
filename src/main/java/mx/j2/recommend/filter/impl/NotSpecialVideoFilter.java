package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @ Author     ：zhongrenli
 * @ Date       ：Created in 下午6:53 2018/12/5
 * @ Description：${description}
 */
public class NotSpecialVideoFilter extends BaseFilter {

    private  static final List<String> APP_SOURCE_LIST = new ArrayList<String>(){
        {
            add("roposo");
            add("tiktok");
            add("vid_status");
            add("vigo");
            add("vmate");
        }
    };

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection baseDc) {
        if (null == doc) {
            return true;
        }

        if (doc.uploadSign == 1) {
            return false;
        }

        if (doc.specialSign == 1) {
            return false;
        }

        if (MXJudgeUtils.isNotEmpty(doc.appName) && APP_SOURCE_LIST.contains(doc.appName)) {
            return false;
        }

        return true;
    }

}
