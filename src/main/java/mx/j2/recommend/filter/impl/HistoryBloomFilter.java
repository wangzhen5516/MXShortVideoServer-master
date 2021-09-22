//package mx.j2.recommend.filter.impl;
//
//import com.newrelic.api.agent.Trace;
//import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
//import mx.j2.recommend.data_model.document.BaseDocument;
//import mx.j2.recommend.data_source.ReBloomDataSource;
//import mx.j2.recommend.manager.DataSourceManager;
//import mx.j2.recommend.util.BloomUtil;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.List;
//
///**
// * @ Author     ：xiang.zhou
// * @ Date       ：Created in 下午6:53 2019/7/7
// * @ Description：布隆过滤器的历史过滤
// */
//public class HistoryBloomFilter extends BaseFilter {
//    private static Logger logger = LogManager.getLogger(HistoryBloomFilter.class);
//
//    @Override
//    @Trace(dispatcher = true)
//    public void filt(BaseDataCollection baseDc) {
//
//        if(baseDc == null || baseDc.req == null || baseDc.req.getUserInfo() == null) {
//            logger.error("get a null UserInfo");
//            return ;
//        }
//
//        List<BaseDocument> delList = Collections.synchronizedList(new ArrayList<>());
//
//        baseDc.uuidBloomKey = BloomUtil.getUuIdBloomKey(baseDc);
//        baseDc.userIdBloomKey = BloomUtil.getUserIdBloomKey(baseDc);
//
//        if(baseDc.uuidBloomKey == null) {
//            logger.error("get a null uuid bloomKey, the UserInfo is " + baseDc.req.getUserInfo());
//            return ;
//        }
//
////        BatchUtil.partitionCall2ListAsync(baseDc.mergedList, 100, null, this::filtHelper);
////        BatchUtil.partitionCall2ListAsync(baseDc.highPriorityManualList, 100, null, this::filtHelper);
//        filtHelper(delList, baseDc.mergedList, baseDc);
//        filtHelper(delList, baseDc.highPriorityManualList, baseDc);
//        filtHelper(delList, baseDc.offlineCalculateRecommendList, baseDc);
//        filtHelper(delList, baseDc.ugcLv1List, baseDc);
//        filtHelper(delList, baseDc.ugcLv2List, baseDc);
//        filtHelper(delList, baseDc.ugcLv3List, baseDc);
//        filtHelper(delList, baseDc.ugcLv4List, baseDc);
//        filtHelper(delList, baseDc.ugcLv5List, baseDc);
//        filtHelper(delList, baseDc.followersContentList, baseDc);
//
//        baseDc.mergedList.removeAll(delList);
//        baseDc.highPriorityManualList.removeAll(delList);
//        baseDc.offlineCalculateRecommendList.removeAll(delList);
//        baseDc.ugcLv1List.removeAll(delList);
//        baseDc.ugcLv2List.removeAll(delList);
//        baseDc.ugcLv3List.removeAll(delList);
//        baseDc.ugcLv4List.removeAll(delList);
//        baseDc.ugcLv5List.removeAll(delList);
//        baseDc.followersContentList.removeAll(delList);
//
//        baseDc.appendToDeletedRecord(delList.size(), this.getName());
//    }
//
//    public List<BaseDocument> filtHelper(List<BaseDocument>toDelListOut, List <BaseDocument> itemList, BaseDataCollection dc) {
//        return filtHelper(toDelListOut, itemList, dc, "");
//    }
//
//    @Trace(dispatcher = true)
//    public List<BaseDocument> filtHelper(List<BaseDocument>toDelListOut, List <BaseDocument> itemList, BaseDataCollection dc, String descSuffix) {
//        if(itemList == null || itemList.isEmpty()) {
//            return itemList;
//        }
//        List<String> itemIds = new ArrayList<>();
//        itemList.forEach((baseDocument)->itemIds.add(baseDocument.id));
//        ReBloomDataSource dataSource = DataSourceManager.INSTANCE.getReBloomDataSource();
//        boolean[] results = dataSource.exists(dc.uuidBloomKey, itemIds.subList(0, itemIds.size() > 200? 200: itemIds.size()).toArray(new String[0]));
//        boolean[] results2 = dataSource.exists(dc.userIdBloomKey, itemIds.subList(0, itemIds.size() > 1000? 1000: itemIds.size()).toArray(new String[itemIds.size()]));
//        if(results == null && results2 == null) {
//            return itemList;
//        }
//        List<BaseDocument> localDel = new ArrayList<>();
//
//        boolean[] mergedResults = mergedResults(results, results2);
//
//        for (int i = 0; i < mergedResults.length && i < itemList.size(); i++) {
//            if (mergedResults[i]) {
//                localDel.add(itemList.get(i));
//            }
//        }
//        dc.appendToDeletedRecord(localDel.size(), this.getName() + descSuffix);
//        toDelListOut.addAll(localDel);
//        return itemList;
//    }
//
//    @Override
//    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {
//        return false;
//    }
//
//    public boolean[] mergedResults(boolean[] result1, boolean[] result2) {
//        if (result1 == null && result2 == null) {
//            return new boolean[0];
//        }
//        if (result1 == null || result1.length == 0) {
//            return result2;
//        }
//        if (result2 == null || result2.length == 0) {
//            return result1;
//        }
//
//        boolean[] mergedResults = new boolean[Math.max(result1.length, result2.length)];
//        if (result1.length == result2.length) {
//            for (int i = 0; i < result1.length; i++) {
//                mergedResults[i] = result1[i] || result2[i];
//            }
//        } else if (result1.length < result2.length) {
//            mergedResults = result2;
//            for (int i = 0; i < result1.length; i++) {
//                mergedResults[i] = result1[i] || result2[i];
//            }
//        } else {
//            mergedResults = result1;
//            for (int i = 0; i < result2.length; i++) {
//                mergedResults[i] = result1[i] || result2[i];
//            }
//        }
//        return mergedResults;
//    }
//}
