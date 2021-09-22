package mx.j2.recommend.ranker.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Qi Mao
 * @ Author     ：Qi Mao
 * @ Date       ：1/4/2021
 * @ Description：${description}
 */

public class EffectTopVideoRanker extends BaseRanker<BaseDataCollection> {

    @Override
    public boolean skip(BaseDataCollection dc) {
        if(dc.effectTopVideoList.isEmpty() || dc.mergedList.isEmpty()){
            return true;
        }
        return false;
    }

    @Override
    public void rank(BaseDataCollection dc) {
        List<String> topVideoId = new ArrayList<>();
        List<BaseDocument> topVideo = dc.effectTopVideoList;

        for(BaseDocument bd : topVideo) {
            topVideoId.add(bd.getId());
        }

        if (MXJudgeUtils.isEmpty(dc.req.nextToken)) {
            List<BaseDocument> mixVideo = new ArrayList<>(topVideo);
            for(BaseDocument bd : dc.mergedList) {
                if(!topVideoId.contains(bd.getId())) {
                    mixVideo.add(bd);
                }
            }

            dc.mergedList.clear();
            dc.mergedList.addAll(mixVideo);
        } else {
            List<BaseDocument> deletebd = new ArrayList<>();
            for(BaseDocument bd : dc.mergedList) {
                if(topVideoId.contains(bd.getId())) {
                    deletebd.add(bd);
                }
            }
            dc.mergedList.removeAll(deletebd);
        }

    }
}
