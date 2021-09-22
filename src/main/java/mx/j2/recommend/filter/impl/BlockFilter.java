package mx.j2.recommend.filter.impl;

import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.hystrix.BlockListHttpCommand;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @ Author     ：DuoZhao
 * @ Date       ：Created in 下午4:05 2020/11/12
 * @ Description：${description}
 */

public class BlockFilter extends BaseFilter {
    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {
        if (MXJudgeUtils.isEmpty(dc.blockList)) {
            return false;
        }

        return dc.blockList.contains(doc.getPublisher_id());
    }

    @Override
    public boolean prepare(BaseDataCollection dc) {
        if (MXStringUtils.isEmpty(dc.req.userInfo.userId) || MXStringUtils.isEmpty(dc.req.userInfo.uuid) || dc.req.userInfo.uuid.equals(dc.req.userInfo.userId)) {
            return false;
        }
        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        String localCacheKey = String.format("%s_%s", this.getName(), dc.req.getUserInfo().getUserId());
        List<String> blockList = localCacheDataSource.getBlockListCache(localCacheKey);

        if (null == blockList) {
            blockList = getBlockList(dc.req.getUserInfo().getUserId());
            if (null == blockList) {
                return false;
            }
            localCacheDataSource.setBlockListCache(localCacheKey, blockList);
        }
        dc.blockList.addAll(blockList);

        return true;
    }

    /**
     * 该用户的拉黑列表
     *
     * @param userId
     * @return list of publisher id
     */
    private List<String> getBlockList(String userId) {
        String result = new BlockListHttpCommand(userId).execute();
        List<String> blockList = new ArrayList<>();
        if (MXJudgeUtils.isEmpty(result)) {
            return blockList;
        }
        JSONObject object;
        try {
            object = JSONObject.parseObject(result);
        } catch (Exception e) {
            e.printStackTrace();
            return blockList;
        }

        if (object.containsKey("list")) {
            blockList = object.getJSONArray("list").toJavaList(String.class);
        }
        return blockList;
    }
}
