package mx.j2.recommend.ruler.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.thrift.Result;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXCollectionUtils;
import mx.j2.recommend.util.MXStringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author qiqi
 * @date 2021-03-31 17:37
 */
public class LivePageResultRuler extends BaseRuler<BaseDataCollection> {


    @Override
    public boolean skip(BaseDataCollection dc) {
        return MXCollectionUtils.isEmpty(dc.data.result.resultList) || MXStringUtils.isBlank(dc.req.getNextToken());
    }

    @Override
    public void rule(BaseDataCollection dc) {


        boolean isFindNextToken = false;
        int index = -1;
        for (Result r : dc.data.result.resultList) {
            String resourceType = r.getResultType();
            String id = "";
            if (DefineTool.CategoryEnum.LIVE_STREAM.getName().equals(resourceType)) {
                id = r.getLiveStream().getStreamId();
            }

            if (dc.req.nextToken.equals(id)) {
                isFindNextToken = true;
                index = dc.data.result.resultList.indexOf(r);
                break;
            }
        }

        if (isFindNextToken && index >= 0) {
            List<Result> newResultList = new ArrayList<>(dc.data.result.resultList.subList(index + 1, dc.data.result.resultList.size()));
            dc.data.result.resultList.clear();
            dc.data.result.resultList.addAll(newResultList);
        }
    }
}
