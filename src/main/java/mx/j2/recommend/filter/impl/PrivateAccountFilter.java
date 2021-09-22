package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author DuoZhao
 * 过滤private account的视频
 */

public class PrivateAccountFilter extends BaseFilter {

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {
        // 当为1时，则视频的publisher为私有账户，应过滤
        return doc.privateAccount == 1;
    }

}