package mx.j2.recommend.filter.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.util.MXStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@SuppressWarnings("unused")
public class UGCSensitiveContentNewFilter extends BaseFilter {
    private static Logger logger = LogManager.getLogger(UGCSensitiveContentNewFilter.class);

    @Override
    @Trace(dispatcher = true)
    public boolean isFilted(BaseDocument doc, BaseDataCollection baseDc) {
        if (doc == null) {
            return true;
        }

        // 非 UGC，不过滤
        if (!doc.isUgc()) {
            return false;
        }

        //如果是看自己主页
        if (baseDc != null && baseDc.req != null && baseDc.req.userInfo != null &&
                MXStringUtils.isNotEmpty(baseDc.req.resourceId) && MXStringUtils.isNotEmpty(baseDc.req.userInfo.userId) &&
                baseDc.req.resourceId.equals(baseDc.req.userInfo.userId)) {
            //新版本直接返回不过滤，交给DI去判断; 老版本过滤掉审核未通过的
            if ((MXStringUtils.isEmpty(baseDc.req.clientVersion) || baseDc.req.clientVersion.compareTo("10032") <= 0) &&
                    ((doc.exists(BaseDocument.FIELD_EXISTS_FLAG_IS_PORN) && (doc.getIsPorn() == 1))
                            || (doc.exists(BaseDocument.FIELD_EXISTS_FLAG_IS_TERRORISM) && (doc.getIsTerrorism() == 1))
                            || (doc.exists(BaseDocument.FIELD_EXISTS_FLAG_IS_POLITICAL) && (doc.getIsPolitical() == 1)))) {
                return true;
            }
        } else {
            //如果是看别人的主页
            //只要不是审核通过的，全过滤
            if (!((doc.exists(BaseDocument.FIELD_EXISTS_FLAG_IS_PORN) && (doc.getIsPorn() == 0))
                    && (doc.exists(BaseDocument.FIELD_EXISTS_FLAG_IS_TERRORISM) && (doc.getIsTerrorism() == 0))
                    && (doc.exists(BaseDocument.FIELD_EXISTS_FLAG_IS_POLITICAL) && (doc.getIsPolitical() == 0)))) {
                return true;
            }
        }
        return false;
    }
}
