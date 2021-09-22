package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;

/**
 * @author zhongrenli
 */
public class RobotVideoFilter extends BaseFilter {
    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection baseDc) {
        if (doc == null) {
            return true;
        }
        if ("snack".equals(doc.appName)) {
            return false;
        }

        return doc.isRobot == 1;
    }
}
