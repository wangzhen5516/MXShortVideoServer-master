package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.hystrix.redis.ZrevRangePvCommand;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;

/**
 * @Author: xiaoling.zhu
 * @Date: 2020-11-03
 */

public class UserDislikePubVideoFilter extends BaseFilter {
    private static final int length = 3;
    private static final String REDIS_KEY_FORMAT = "%s:dislike_publisher";

    @Override
    public boolean prepare(BaseDataCollection dc) {
        if(dc.userDislikePubIds!=null){
            dc.userDislikePubIds.clear();
        }
        String userID = dc.client.user.uuId;
        if(MXStringUtils.isNotEmpty(userID)){
            ZrevRangePvCommand userPubHystrixCommand = new ZrevRangePvCommand(String.format(REDIS_KEY_FORMAT, userID), 0, length);
            dc.userDislikePubIds = userPubHystrixCommand.execute();
        }

        return true;
    }

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {
        if(MXJudgeUtils.isNotEmpty(dc.userDislikePubIds)){
            if(dc.userDislikePubIds.contains(doc.publisher_id)){
                return true;
            }
        }
        return false;
    }
}
