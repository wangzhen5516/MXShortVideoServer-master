package mx.j2.recommend.packer.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BannerDataCollection;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.thrift.Banner;
import mx.j2.recommend.util.DefineTool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * @author xiang.zhou
 * @date 2018/8/19
 */
public class Mx_Recommend_Banner_Packer extends BasePacker{

    @Override
    @Trace(dispatcher = true)
    public void pack(BaseDataCollection baseDc){

        if(!(baseDc instanceof BannerDataCollection)) {
            return;
        }
        BannerDataCollection dc = (BannerDataCollection) baseDc;

        for (int i = 0; i < dc.mergedList.size(); i++) {
            BaseDocument doc = dc.mergedList.get(i);

            if (doc == null) {
                continue;
            }
            packResult(dc, doc, dc.bannerList);
        }
    }

    private Banner packResult(BaseDataCollection dc, BaseDocument doc, List<Banner> resultList) {
        Banner r = new Banner();

        if (DefineTool.CategoryEnum.BANNER.equals(doc.category)) {
            r.setBannerId(doc.id);
            resultList.add(r);
            return  r;
        }
        return null;
    }

}
