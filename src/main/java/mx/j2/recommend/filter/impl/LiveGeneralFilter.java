package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.document.LiveDocument;
import mx.j2.recommend.util.MXCollectionUtils;
import mx.j2.recommend.util.MXStringUtils;

import java.util.Map;

/**
 * @author qiqi
 * @date 2021-04-26 10:59
 */
public class LiveGeneralFilter extends BaseFilter<BaseDataCollection> {

    private String configFiled = "ratio";
    private int ratio = 20;

    @Override
    public boolean skip(BaseDataCollection dc) {
        if (MXCollectionUtils.isEmpty(dc.liveDocumentList)) {
            return true;
        }
        String userId = dc.req.getUserInfo().getUserId();
        ratio = config.getInt(configFiled);
        /**
         * 如果当前用户命中流量区间直接skip
         */
        return MXStringUtils.isNotBlank(userId) && Math.abs(userId.hashCode()) % 100 < ratio;
    }


    @Override
    public void registerConfig(Map outConfMap) {
        outConfMap.put(configFiled, Integer.class);
    }

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {

        LiveDocument document = (LiveDocument) doc;
        /**
         * 1.该直播间是普通的
         * 2.不是该用户关注的（暂时将普通直播间全部下掉（包括关注））
         */
        return document.getLiveWhiteList() == 0;
    }
}
