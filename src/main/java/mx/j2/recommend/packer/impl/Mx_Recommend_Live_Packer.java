package mx.j2.recommend.packer.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.document.LiveDocument;
import mx.j2.recommend.thrift.InternalUse;
import mx.j2.recommend.thrift.LiveStream;
import mx.j2.recommend.thrift.Result;
import mx.j2.recommend.util.DefineTool;

import java.util.List;

/**
 * @author qiqi
 * @date 2021-03-30 18:53
 */
public class Mx_Recommend_Live_Packer extends BasePacker {

    @Override
    @Trace(dispatcher = true)
    public void pack(BaseDataCollection dc) {
        for (int i = 0; i < dc.mergedList.size(); i++) {
            BaseDocument doc = dc.mergedList.get(i);

            if (doc == null) {
                continue;
            }
            packResult(dc, doc, dc.data.result.resultList);
        }
    }

    private void packResult(BaseDataCollection dc, BaseDocument doc, List<Result> resultList) {

        Result r = new Result();
        r.setResultType(doc.category.getName());
        InternalUse internalUse = new InternalUse();
        internalUse.setNextToken(doc.nextTokenMap.get(dc.req.interfaceName));
        r.setInternalUse(internalUse);
        if (DefineTool.CategoryEnum.LIVE_STREAM.equals(doc.category)) {

            LiveDocument document = (LiveDocument) doc;
            LiveStream liveStream = new LiveStream();
            liveStream.setStreamId(document.getStreamId());
            liveStream.setPublisherId(document.getPublisher_id());
            r.setLiveStream(liveStream);
            if (dc.isDebugModeOpen) {
                String debugInfo = packDebugInfo(document).toString();
                r.setDebugInfo(debugInfo);
            }
            resultList.add(r);
        }
    }

    private StringBuilder packDebugInfo(LiveDocument doc) {
        StringBuilder debugInfo = new StringBuilder();
        debugInfo.append("recall: ");
        debugInfo.append(doc.recallName);
        debugInfo.append(",");
        debugInfo.append("\nstreamId: ").append(doc.getStreamId()).append(",");
        debugInfo.append("\nliveOrder: ").append(doc.getLiveOrder()).append(",");
        debugInfo.append("\nliveScore: ").append(doc.getLiveScore()).append(",");
        debugInfo.append("\nisWhite: ").append(doc.getLiveWhiteList()).append(",");
        debugInfo.append("\npublisherId: ").append(doc.getPublisher_id()).append(",");
        debugInfo.append("\nisFollowed: ").append(doc.isFollow()).append(",");
        debugInfo.append("\nliveLanguage: ").append(doc.getLiveLanguageLists());
        return debugInfo;
    }
}
