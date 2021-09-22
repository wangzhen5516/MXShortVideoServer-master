package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.document.ShortDocument;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;

/**
 * 州过滤器
 */
@SuppressWarnings("unused")
public class StateFilter extends BaseFilter {

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection baseDc) {
        if (null == doc) {
            return true;
        }

        if (!(doc instanceof ShortDocument)) {
            return true;
        }

        ShortDocument shortDocument = (ShortDocument) doc;

        if (MXJudgeUtils.isEmpty(shortDocument.filterStates)) {
            return false;
        }

        if (DefineTool.TabInfoEnum.HOT.getId().equals(baseDc.req.getTabId())) {
            return shortDocument.filterStates.contains(baseDc.req.location.state);
        }

        return false;
    }
}
