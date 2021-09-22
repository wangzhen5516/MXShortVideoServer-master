package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.manager.DataSourceManager;
import mx.j2.recommend.task.FillBloomFilterInfoTask;
import mx.j2.recommend.task.TaskExecutor;
import mx.j2.recommend.thrift.UserInfo;
import mx.j2.recommend.util.OptionalUtil;
import mx.j2.recommend.util.bean.BaseBloomFilter;
import mx.j2.recommend.util.bean.BloomFilterCollections;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static mx.j2.recommend.util.DefineTool.UserHistoryBloomInfoEnum;

/**
 * @author ：zhongrenli
 * @date ：Created in 11:34 上午 2020/10/27
 */
public class UserRecommendBloomFilter extends BaseFilter {

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {

        if (Optional.ofNullable(dc.bloomFilterCollections.getUserIdHistoryBloomFilter()).isPresent()) {
            if (dc.bloomFilterCollections.getUserIdHistoryBloomFilter().mightContain(doc.id)) {
                dc.recallFilterCount.add("userIdHistoryBloom");
                return true;
            }
        }
        if (Optional.ofNullable(dc.bloomFilterCollections.getUuidHistoryBloomFilter()).isPresent()) {
            if (dc.bloomFilterCollections.getUuidHistoryBloomFilter().mightContain(doc.id)) {
                dc.recallFilterCount.add("uuIdHistoryBloom");
                return true;
            }
        }
        if (Optional.ofNullable(dc.bloomFilterCollections.getAdvertiseIdBloomFilter()).isPresent()) {
            if (dc.bloomFilterCollections.getAdvertiseIdBloomFilter().mightContain(doc.id)) {
                dc.recallFilterCount.add("adIdHistoryBloom");
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean prepare(BaseDataCollection dc) {
        loadHistory(dc);
        return true;
    }

    private void loadHistory(BaseDataCollection baseDc) {
        Map<String, String> map = makeMap(baseDc);
        if (map.isEmpty()) {
            System.out.println("UserInfo is empty! " + baseDc.req);
            return;
        }

        boolean isVipUser = false;
        long timeout = 500;
//        if (baseDc.isDebugModeOpen) {
//            isVipUser = true;
//            timeout = 800;
//        }

        TaskExecutor executor = DataSourceManager.INSTANCE.getTaskExecutor();
        FillBloomFilterInfoTask fillBloomFilterInfoTask = new FillBloomFilterInfoTask(executor);

        BaseBloomFilter userIdFilter = new BaseBloomFilter.UserIdHistoryBloomFilter();
        BaseBloomFilter uuIdFilter = new BaseBloomFilter.UuidHistoryBloomFilter();
        BaseBloomFilter adIdFilter = new BaseBloomFilter.AdvertiseHistoryBloomFilter();

        CountDownLatch cd = new CountDownLatch(map.size());
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (UserHistoryBloomInfoEnum.USER_ID.getName().equals(entry.getKey())) {
                fillBloomFilterInfoTask.execute(userIdFilter, cd, entry.getValue(), entry.getKey(), isVipUser);
            }

            if (UserHistoryBloomInfoEnum.UUID.getName().equals(entry.getKey())) {
                fillBloomFilterInfoTask.execute(uuIdFilter, cd, entry.getValue(), entry.getKey(), isVipUser);
            }

            if (UserHistoryBloomInfoEnum.AD_ID.getName().equals(entry.getKey())) {
                fillBloomFilterInfoTask.execute(adIdFilter, cd, entry.getValue(), entry.getKey(), isVipUser);
            }
        }

        try {
            cd.await(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        baseDc.bloomFilterCollections.setUserIdHistoryBloomFilter(userIdFilter.getBloomFilter());
        baseDc.bloomFilterCollections.setUserIdHistoryBloomSize(userIdFilter.getBloomSize());
        baseDc.bloomFilterCollections.setUuidHistoryBloomFilter(uuIdFilter.getBloomFilter());
        baseDc.bloomFilterCollections.setUuidHistoryBloomSize(uuIdFilter.getBloomSize());
        baseDc.bloomFilterCollections.setAdvertiseIdBloomFilter(adIdFilter.getBloomFilter());
        baseDc.bloomFilterCollections.setAdvertiseIdBloomSize(adIdFilter.getBloomSize());

        baseDc.userHistorySize = max(baseDc.bloomFilterCollections);
        baseDc.userHistorySize += baseDc.historyIdList.size();
    }

    private long max(BloomFilterCollections collections) {
        long max = Math.max(collections.getUserIdHistoryBloomSize(), collections.getUuidHistoryBloomSize());
        return Math.max(max, collections.getAdvertiseIdBloomSize());
    }

    private Map<String, String> makeMap(BaseDataCollection baseDc) {
        UserInfo userInfo = baseDc.req.userInfo;
        Map<String, String> map = new HashMap<>(16);
        OptionalUtil.ofNullable(userInfo)
                .getUtil(UserInfo::getUserId)
                .ifPresent(id -> map.put(UserHistoryBloomInfoEnum.USER_ID.getName(), id));

        OptionalUtil.ofNullable(userInfo)
                .getUtil(UserInfo::getUuid)
                .ifPresent(id -> map.put(UserHistoryBloomInfoEnum.UUID.getName(), id));

        OptionalUtil.ofNullable(userInfo)
                .getUtil(UserInfo::getAdId)
                .ifPresent(id -> map.put(UserHistoryBloomInfoEnum.AD_ID.getName(), id));

        if (OptionalUtil.ofNullable(userInfo).isPresent()) {
            if (userInfo.userId.equals(userInfo.uuid)) {
                map.remove(UserHistoryBloomInfoEnum.UUID.getName());
            }
        }
        return map;
    }
}
