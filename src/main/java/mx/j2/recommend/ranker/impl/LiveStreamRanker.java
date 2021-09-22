package mx.j2.recommend.ranker.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.LiveDocument;
import mx.j2.recommend.util.MXCollectionUtils;
import mx.j2.recommend.util.MXStringUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @author qiqi
 * @date 2021-03-24 15:50
 */
public class LiveStreamRanker extends BaseRanker<BaseDataCollection> {


    @Override
    public boolean skip(BaseDataCollection dc) {
        return false;
    }

    @Override
    public void rank(BaseDataCollection dc) {
        dc.mergedList.clear();
        if (MXCollectionUtils.isNotEmpty(dc.liveDocumentList)) {
            dc.liveDocumentList.sort(Comparator.comparingDouble(LiveDocument::getLiveScore).reversed());
        }
        /*进行置顶操作*/
        topResult(dc);
        if (MXCollectionUtils.isEmpty((dc.liveDocumentList))) {
            return;
        }
        dc.mergedList.addAll(dc.liveDocumentList);
    }

    /**
     * 执行置顶操作
     *
     * @param dc
     */
    private void topResult(BaseDataCollection dc) {
        if (MXCollectionUtils.isEmpty(dc.liveLockDocumentList)) {
            return;
        }
        dc.liveLockDocumentList.sort(Comparator.comparing(d -> Integer.parseInt(d.getLiveOrder())));
        List<LiveDocument> resDoc = new ArrayList<>(dc.liveDocumentList);
        for (LiveDocument doc : dc.liveLockDocumentList) {
            if (MXStringUtils.isNotBlank(doc.getStreamId())) {
                int order = Integer.parseInt(doc.getLiveOrder());
                if (order > resDoc.size()) {
                    resDoc.add(doc);
                } else {
                    resDoc.add(order - 1, doc);
                }
            }
        }
        dc.liveDocumentList.clear();
        dc.liveDocumentList.addAll(resDoc);
    }
}
