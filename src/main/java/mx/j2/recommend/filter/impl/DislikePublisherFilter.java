package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.data_source.UserProfileDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.BloomUtil;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.List;

public class DislikePublisherFilter extends BaseFilter {

    public static final String FIELD_NAME = "unlike_publisher_id";

    @Override
    public boolean prepare(BaseDataCollection baseDc) {
        if (!DefineTool.TabInfoEnum.HOT.getId().equals(baseDc.req.tabId)) {
            return false;
        }

        String cacheKey = construcCacheKey(baseDc);

        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        List<String> cacheList = localCacheDataSource.getDisLikePublisherCache(cacheKey);

        if (MXJudgeUtils.isNotEmpty(cacheList)) {
            baseDc.disLikePublisherIdList.addAll(cacheList);
            baseDc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.LOCAL.getName());
            baseDc.syncSearchResultSizeMap.put(this.getName(), cacheList.size());
            return true;
        }

        UserProfileDataSource dataSource = MXDataSource.profile();
        String userProfile = dataSource.getUserProfileByUuId(baseDc.client.user.uuId);

        List<String> ids = dataSource.getUserProfileByField(userProfile, FIELD_NAME);
        if(MXJudgeUtils.isEmpty(ids)){
            return false;
        }

        localCacheDataSource.setDisLikePublisherCache(cacheKey, ids);
        baseDc.disLikePublisherIdList.addAll(ids);
        baseDc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.CASSANDRA.getName());
        baseDc.syncSearchResultSizeMap.put(this.getName(), ids.size());

        return true;
    }

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {
        if (doc == null) {
            return true;
        }
        if (MXJudgeUtils.isEmpty(dc.disLikePublisherIdList)) {
            return false;
        }
        return dc.disLikePublisherIdList.contains(doc.publisher_id);
    }

    private String construcCacheKey(BaseDataCollection baseDc){
        return String.format("%s_%s_%s", this.getName(), baseDc.req.getInterfaceName(), BloomUtil.getUuid(baseDc));
    }
}
