package mx.j2.recommend.predictor.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_source.AmazonSageDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;

/**
 * @author ：zhongrenli
 * @date ：Created in 2:57 下午 2020/12/18
 */
public class FirstPredictor extends BasePredictor<BaseDataCollection> {
    private final static String ENDPOINT_NAME = "prod-taka-xgboost-v1";

    @Override
    public void predict(BaseDataCollection dc) {
        if (MXJudgeUtils.isEmpty(dc.predictDocumentList)) {
            return;
        }
        String featureStr = getFeatureString(dc);
        if(MXStringUtils.isEmpty(featureStr)){
            return;
        }
        dc.featureString = featureStr;

        AmazonSageDataSource dataSource = MXDataSource.sage();
        String result = dataSource.sendInAsync(dc, ENDPOINT_NAME);
        if (MXStringUtils.isEmpty(result)) {
            return;
        }

        String[] scores = result.split(",");
        for (int i = 0; i < dc.predictDocumentList.size() && i < scores.length; i++) {
            dc.predictDocumentList.get(i).scoreDocument.setPredictScore(Double.parseDouble(scores[i]));
        }
        dc.predictDocumentList.sort((o1, o2) -> Double.compare(o2.scoreDocument.getPredictScore(), o1.scoreDocument.getPredictScore()));
    }
}
