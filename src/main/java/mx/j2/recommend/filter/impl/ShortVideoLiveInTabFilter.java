package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.document.ShortDocument;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;

/**
 * @ Author     ：zhongrenli
 * @ Date       ：Created in 下午6:53 2018/12/5
 * @ Description：过滤掉不应该在该页面展示的数据
 */
public class ShortVideoLiveInTabFilter extends BaseFilter {

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection baseDc) {
        if (null == doc) {
            return true;
        }
        if (!(doc instanceof ShortDocument)) {
            return true;
        }
        ShortDocument shortDocument = (ShortDocument)doc;

        if (MXJudgeUtils.isEmpty(shortDocument.liveInTabArray)) {
            return false;
        }

        if (DefineTool.TabInfoEnum.STATUS.getId().equals(baseDc.req.getTabId())) {
            if (shortDocument.liveInTabArray.contains(DefineTool.TabInfoEnum.STATUS.getName())) {
                return false;
            }
            return true;
        }

        if (DefineTool.TabInfoEnum.HOT.getId().equals(baseDc.req.getTabId())) {
            if (shortDocument.liveInTabArray.contains(DefineTool.TabInfoEnum.HOT.getName())) {
                return false;
            }
            return true;
        }

        return false;
    }

}
