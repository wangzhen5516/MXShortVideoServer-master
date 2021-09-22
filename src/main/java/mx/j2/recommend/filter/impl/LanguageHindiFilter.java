package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXCollectionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @ Author     ：xiang.zhou
 * @ Date       ：Created in 下午6:53 2020/10/5
 */
public class LanguageHindiFilter extends BaseFilter {

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection baseDc) {
        if (null == doc) {
            return true;
        }

        if (baseDc == null || baseDc.req == null || baseDc.req.languageList == null) {
            return false;
        }

        if (!baseDc.req.languageList.contains("Hindi")) {
            return false;
        }

        if (MXCollectionUtils.isEmpty(doc.languageIdList) || doc.languageIdList.contains(DefineTool.Language.NoLanguage.id)) {
            return false;
        }

        List<String> reqLanguageListSource = baseDc.req.languageList;
        List<String> reqLanguageList = new ArrayList<>();
        for (String s : reqLanguageListSource) {
            reqLanguageList.add(DefineTool.Language.findLanaguage(s, DefineTool.Language.NoLanguage).id);
        }

        List<String> res = new ArrayList<>(doc.languageIdList);
        res.retainAll(reqLanguageList);
        /*size==0表示没交集*/
        return res.size() == 0;
    }

}
