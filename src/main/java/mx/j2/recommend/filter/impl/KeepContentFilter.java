package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;

import java.util.ArrayList;
import java.util.List;

public class KeepContentFilter extends BaseFilter {
    private static List<String> keepCon = new ArrayList();
    static {
        keepCon.add("gifskey");
        keepCon.add("dp_and_status");
        keepCon.add("quotes_in_hindi");
        keepCon.add("good_night_devotional");
        keepCon.add("hindi_dp_status");
        keepCon.add("superb");
        keepCon.add("shareblast");
        keepCon.add("instagram");
        keepCon.add("topbuzz");
        keepCon.add("gifkaro");
    }

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection baseDc) {
        if (null == doc) {
            return true;
        }

        if (!keepCon.contains(doc.appName)) {
            return true;
        }

        return false;
    }

}
