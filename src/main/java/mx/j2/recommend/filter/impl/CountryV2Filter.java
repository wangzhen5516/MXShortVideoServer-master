package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.util.MXCollectionUtils;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.*;

/**
 * @author qiqi
 * @date 2021-02-07 14:26
 */
public class CountryV2Filter extends BaseFilter {
    /**
     * 三个特定的state(Telugu,Tamil,Bengali)
     */
    private static final Set<String> STATE_LANG = new HashSet<>(Arrays.asList("te", "ta", "bn"));

    private static final String COUNTRY_NAME = "IND";

    private static final Set<String> BLACK_LIST = new HashSet<>(Arrays.asList("PAK", "BGD"));


    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {
        if (doc == null) {
            return true;
        }
        List<String> countryList = doc.countries;
        if (MXJudgeUtils.isEmpty(countryList) || countryList.contains(COUNTRY_NAME)) {
            return false;
        }

        if (MXCollectionUtils.isEmpty(doc.languageIdList)) {
            return false;
        }
        if (MXCollectionUtils.isEmpty(dc.req.languageList)) {
            return true;
        }
        /*当前视频的语言和请求的语言列表没交集则过滤*/
        List<String> res = new ArrayList<>(dc.req.languageList);
        res.retainAll(doc.languageIdList);
        /*size==0表示没交集*/
        if (res.size() == 0) {
            return true;
        }
        /*交集是否与特定的三个州有交集*/
        res.retainAll(STATE_LANG);
        return res.size() == 0;
    }
}


