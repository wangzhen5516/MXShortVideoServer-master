package mx.j2.recommend.filter.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @ Author     ：zhongrenli
 * @ Date       ：Created in 下午6:53 2018/12/5
 * @ Description：针对运营特殊包，若选了特殊app来源，
 *                过滤掉不在选择的来源之内的视频
 */
public class NotSelectAppSourceFilter extends BaseFilter<BaseDataCollection> {

    @Override
    public boolean skip(BaseDataCollection baseDc) {
        if (MXJudgeUtils.isEmpty(baseDc.req.appSourceList)) {
            return true;
        }
        return false;
    }

    @Override
    @Trace(dispatcher = true)
    public void filter(BaseDataCollection baseDc) {
        List<BaseDocument> deleted = new ArrayList<>();

        for (BaseDocument doc : baseDc.mergedList) {
            if (isFilted(doc, baseDc)) {
                deleted.add(doc);
            }
        }

        baseDc.appendToDeletedRecord(deleted.size(), this.getName());
        baseDc.mergedList.removeAll(deleted);
        deleted.clear();
    }

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection baseDc) {
        if (null == doc) {
            return true;
        }

        if (MXJudgeUtils.isEmpty(doc.appName)) {
            return true;
        }

        if (!baseDc.req.appSourceList.contains(doc.appName)) {
            if (baseDc.req.appSourceList.contains("tiktok_special") && "tiktok".equals(doc.appName)) {
                return false;
            }
            return true;
        }

        return  false;
    }

}
