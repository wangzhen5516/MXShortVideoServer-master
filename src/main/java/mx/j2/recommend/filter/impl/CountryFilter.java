package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 如果视频是孟加拉或者巴基斯坦的, 直接过滤掉, 否则保留, 对所有国家的请求生效(by zxj, @2020.10.10)
 *
 * @author qiqi
 * @date 2020-07-14 11:16
 */
public class CountryFilter extends BaseFilter {

    private static final String IND_NAME = "IND";

    private static final Set<String> BLACK_LIST = new HashSet<String>(Arrays.asList("PAK", "BGD"));


    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection baseDc) {
        if (doc == null) {
            return true;
        }

        List<String> countryList = doc.countries;

        if (MXJudgeUtils.isEmpty(countryList) || countryList.contains(IND_NAME)) {
            return false;
        } else {
            for (String country : countryList) {
                if (BLACK_LIST.contains(country)) {
                    return true;
                }
            }
        }
        return false;
    }
}
