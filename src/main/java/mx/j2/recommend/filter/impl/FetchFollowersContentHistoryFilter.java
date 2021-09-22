package mx.j2.recommend.filter.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.ReBloomDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.BloomUtil;
import mx.j2.recommend.util.MXJudgeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 必须放在最后
 */
public class FetchFollowersContentHistoryFilter extends BaseFilter<BaseDataCollection> {
    private static Logger logger = LogManager.getLogger(FetchFollowersContentHistoryFilter.class);

    @Override
    public boolean skip(BaseDataCollection baseDc) {
        if(baseDc == null || baseDc.req == null || baseDc.req.getUserInfo() == null) {
            logger.error("get a null UserInfo");
            return true;
        }
        return false;
    }

    @Override
    @Trace(dispatcher = true)
    public void filter(BaseDataCollection baseDc) {
        List<BaseDocument> delList = Collections.synchronizedList(new ArrayList<>());
        List<BaseDocument> followDelList = Collections.synchronizedList(new ArrayList<>());

        filtHelper(delList, baseDc.mergedList, baseDc);

        int totalIdSize = baseDc.mergedList.size();
        int delSize = delList.size();
        if(totalIdSize>0){
            if((totalIdSize-delSize)>baseDc.req.num){
                baseDc.mergedList.removeAll(delList);
                StringBuilder stringBuilder = new StringBuilder();
                for (BaseDocument document : baseDc.mergedList) {
                    stringBuilder.append("{id = "+document.id+" }, ");
                }
                stringBuilder.append("\n");
            }else {
                if (MXJudgeUtils.isNotEmpty(baseDc.followGuaranteeList)) {
                    filtHelper(followDelList, baseDc.followGuaranteeList, baseDc);
                    if ((totalIdSize - delSize) + (baseDc.followGuaranteeList.size() - followDelList.size()) > baseDc.req.num) {
                        baseDc.mergedList.removeAll(delList);
                        baseDc.followGuaranteeList.removeAll(followDelList);
                    } else {
                        String fetchFollowersBloomKey = BloomUtil.getUserFetchFollowersContentBloomKey(baseDc);
                        MXDataSource.rebloom().deleteBloom(fetchFollowersBloomKey);
                    }
                } else {
                    String fetchFollowersBloomKey = BloomUtil.getUserFetchFollowersContentBloomKey(baseDc);
                    MXDataSource.rebloom().deleteBloom(fetchFollowersBloomKey);
                }
            }
        }

    }

    public List<BaseDocument> filtHelper(List<BaseDocument>toDelListOut, List <BaseDocument> itemList, BaseDataCollection dc) {
        return filtHelper(toDelListOut, itemList, dc, "");
    }

    @Trace(dispatcher = true)
    public List<BaseDocument> filtHelper(List<BaseDocument>toDelListOut, List <BaseDocument> itemList, BaseDataCollection dc, String descSuffix) {
        if (MXJudgeUtils.isEmpty(itemList)) {
            return itemList;
        }
        List<String> itemIds = new ArrayList<>();
        itemList.forEach((baseDocument)->itemIds.add(baseDocument.id));
        ReBloomDataSource dataSource = MXDataSource.rebloom();
        String fetchFollowersBloomKey = BloomUtil.getUserFetchFollowersContentBloomKey(dc);
        boolean[] results = dataSource.exists(fetchFollowersBloomKey, itemIds.toArray(new String[itemIds.size()]));
        if(results == null ) {
            return itemList;
        }
        List<BaseDocument> localDel = new ArrayList<>();


        for (int i = 0; i < results.length && i < itemList.size(); i++) {
            if (results[i]) {
                localDel.add(itemList.get(i));
            }
        }
        dc.appendToDeletedRecord(localDel.size(), this.getName() + descSuffix);
        toDelListOut.addAll(localDel);
        return itemList;
    }

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {
        return false;
    }
}
