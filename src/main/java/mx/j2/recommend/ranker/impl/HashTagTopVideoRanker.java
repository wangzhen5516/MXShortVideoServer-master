package mx.j2.recommend.ranker.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.util.MXStringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author DuoZhao
 * @ Author     ：DuoZhao
 * @ Date       ：Created in 下午8:42 2020/09/08
 */

public class HashTagTopVideoRanker extends BaseRanker<BaseDataCollection> {

    @Override
    public boolean skip(BaseDataCollection dc) {
        if (dc.hashTagTopVideoList.isEmpty() || dc.mergedList.isEmpty()) {
            return true;
        }
        return false;
    }

    @Override
    @Trace(dispatcher = true)
    public void rank(BaseDataCollection dc) {
        List<BaseDocument> topVideo = dc.hashTagTopVideoList;
        for (BaseDocument doc : topVideo) {
            doc.nextTokenMap.put(dc.req.interfaceName, doc.getId());
        }
        List<String> topVideoId = topVideo.stream().map(BaseDocument::getId).collect(Collectors.toList());

        if (MXStringUtils.isNotEmpty(dc.req.nextToken) && dc.req.nextToken.split("\\|").length == 2) {
            List<BaseDocument> deletebd = dc.mergedList.stream().filter(baseDocument -> topVideoId.contains(baseDocument.getId())).collect(Collectors.toList());
            dc.mergedList.removeAll(deletebd);
        } else {
            if (MXStringUtils.isNotEmpty(dc.req.nextToken) && dc.req.nextToken.split("\\|").length == 1) {
                BaseDocument lastVideoDoc = topVideo.stream().filter(baseDocument -> baseDocument.getId().equals(dc.req.nextToken)).findAny().get();
                topVideo = topVideo.subList(topVideo.indexOf(lastVideoDoc) + 1, topVideo.size());
            }

            List<BaseDocument> mixVideo = new ArrayList<>(topVideo);
            for (BaseDocument bd : dc.mergedList) {
                if (!topVideoId.contains(bd.getId())) {
                    mixVideo.add(bd);
                }
            }

            dc.mergedList.clear();
            dc.mergedList.addAll(mixVideo);
        }
    }
}
