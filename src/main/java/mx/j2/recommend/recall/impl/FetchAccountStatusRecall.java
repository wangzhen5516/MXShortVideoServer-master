package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_source.ElasticSearchDataSource;
import mx.j2.recommend.manager.MXDataSource;

public class FetchAccountStatusRecall extends BaseRecall<BaseDataCollection> {
    private static final String REQUEST_URL_FORMAT = "/%s/_search?pretty=false";
    private static final String INDEX_URL = "takatak_cms/snack_transfer";

    @Override
    public boolean skip(BaseDataCollection data) {
        return false;
    }

    @Override
    public void recall(BaseDataCollection baseDc) {

        String queryBody = constructQueryBody(baseDc);

        String EsRequest = String.format(REQUEST_URL_FORMAT, INDEX_URL);
        ElasticSearchDataSource elasticSearchDataSource = MXDataSource.ES();
        Integer status = elasticSearchDataSource.sendSyncSearchForAccountStatus(EsRequest, queryBody);
        if (status != null) {
            baseDc.status = status;
        }
    }

    private String constructQueryBody(BaseDataCollection baseDc) {
        JSONObject content = new JSONObject();
        JSONObject match = new JSONObject();
        JSONObject publisherId = new JSONObject();

        publisherId.put("publisher_id", baseDc.client.user.userId);
        match.put("match", publisherId);
        content.put("query", match);

        return content.toString();
    }
}
