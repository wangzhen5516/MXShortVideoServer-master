package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.util.DefineTool;

/**
 * @ Author     ：zhongrenli
 * @ Date       ：Created in 下午6:53 2018/12/5
 * @ Description：${description}
 */
public class OfflineStatusFilter extends BaseFilter {

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection baseDc) {
        if (null == doc) {
            return true;
        }
        // 目前用户数据不区分上下线状态, 在线处理时一律按上线处理, 修改于2020.07.11, by雪键
//        if (BaseMagicValueEnum.UGC_TAG.equals(doc.videoSource)) {
//            return false;
//        }

        if (DefineTool.OnlineStatusesEnum.ONLINE.getIndex() == doc.status) {
            return false;
        }
        return true;
    }

}
