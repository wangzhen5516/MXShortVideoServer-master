package mx.j2.recommend.ruler.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.thrift.Result;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhongrenli
 * @ Author     ：zhongrenli
 * @ Date       ：Created in 下午5:24 2019/4/11
 * @ Description：用于分页
 */


public class PageResultRuler extends BaseRuler<BaseDataCollection> {

    @Override
    public boolean skip(BaseDataCollection dc) {
        return MXJudgeUtils.isEmpty(dc.req.nextToken);
    }

    @Override
    @Trace(dispatcher = true)
    public void rule(BaseDataCollection dc) {
        List<Result> resultsMergeList = new ArrayList<>();

        boolean isFindNextToken = false;
        int index = -1;
        for (Result r : dc.data.result.resultList) {
            String resourceType = r.getResultType();
            String id = "";
            if (DefineTool.CategoryEnum.SHORT_VIDEO.getName().equals(resourceType)) {
                id = r.getShortVideo().getId();
            }

            if (dc.req.nextToken.equals(id)) {
                isFindNextToken = true;
                index = dc.data.result.resultList.indexOf(r);
                break;
            }
        }

        if (isFindNextToken && index > 0) {
            resultsMergeList.addAll(dc.data.result.resultList.subList(index + 1, dc.data.result.resultList.size()));
            dc.data.result.resultList.clear();
            dc.data.result.resultList.addAll(resultsMergeList);
        }
    }
}
