package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;

/**
 * @author qiqi
 * @date 2020-12-12 17:01
 */
public class NoVoiceBigVFilter extends BaseFilter {

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {
        if (doc == null) {
            return true;
        }
        /*来自腾讯sdk且没有声音*/
        boolean tecentAudio = doc.fromTecent == 1 && doc.audioDuration == 0;
        return doc.isBigV && tecentAudio;
    }
}
