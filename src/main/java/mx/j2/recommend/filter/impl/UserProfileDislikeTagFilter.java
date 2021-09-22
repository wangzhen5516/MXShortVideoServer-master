package mx.j2.recommend.filter.impl;

import com.alibaba.fastjson.JSONArray;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.UserProfileRealtimeDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.*;

import java.util.List;

/**
 * @author xuejian.zhang
 * @date 2021-03-20 14:38
 */
public class UserProfileDislikeTagFilter extends BaseFilter<BaseDataCollection> {

    @Override
    public boolean prepare(BaseDataCollection dc) {
        if (!DefineTool.TabInfoEnum.HOT.getId().equals(dc.req.tabId)) {
            return false;
        }

        UserProfileRealtimeDataSource dataSource = MXDataSource.profileRealtime();
        List<String> tagIds = dataSource.getUnRecommendTag(dc.client.user.uuId);
        if(MXJudgeUtils.isEmpty(tagIds)){
            return false;
        }

        dc.dislikeTagIdList.addAll(tagIds);
        dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.CASSANDRA.getName());
        dc.syncSearchResultSizeMap.put(this.getName(), tagIds.size());

        return true;
    }

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {
        if (doc == null) {
            return true;
        }

        if (doc.getPrimaryTags() == null || doc.getPrimaryTags().isEmpty()) {
            return false;
        }

        for (String toCheck : dc.dislikeTagIdList) {
            if (doc.getPrimaryTags().contains(toCheck)) {
                return true;
            }
        }

        return false;
    }

    private String construcCacheKey(BaseDataCollection dc){
        return String.format("%s_%s_%s", this.getName(), dc.req.getInterfaceName(), dc.req.userInfo.uuid);
    }

    public static void main(String[] args) {
        JSONArray ja = new JSONArray();
        ja.add("zhou");
        ja.add("zhou");
        ja.add("zhou");
        ja.add("xiang");
        boolean a = ja.contains("zhou");
        boolean a1 = ja.contains("xiang");
        boolean a2 = ja.contains("[");
        System.out.println(a);
        System.out.println(a1);
        System.out.println(a2);
    }

}
