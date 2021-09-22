package mx.j2.recommend.data_source;

import mx.j2.recommend.manager.MXManager;
import mx.j2.recommend.recall.impl.ProfilePoolRecall;
import mx.j2.recommend.recall_data_in_mem.ESCanGetStartRecall;

/**
 * 个性化池数据源
 */
public class ProfilePoolDataSource extends BaseExposurePoolDataSource {

    @Override
    String getPoolID() {
        return "Profile";
    }

    @Override
    ESCanGetStartRecall getRecall() {
        return (ESCanGetStartRecall) MXManager.recall().getComponentInstance(ProfilePoolRecall.class.getSimpleName());
    }
}
