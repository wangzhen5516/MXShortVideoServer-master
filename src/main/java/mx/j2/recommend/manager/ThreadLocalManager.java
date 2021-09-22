package mx.j2.recommend.manager;

import mx.j2.recommend.data_model.data_collection.*;

/**
 * @author ：zhongrenli
 * @date ：Created in 4:38 下午 2020/10/12
 */
public class ThreadLocalManager {
    private static ThreadLocal<BannerDataCollection> bannerThreadLocal = ThreadLocal.withInitial(BannerDataCollection::new);
    private static ThreadLocal<FeedDataCollection> feedThreadLocal = ThreadLocal.withInitial(FeedDataCollection::new);
    private static ThreadLocal<OtherDataCollection> otherThreadLocal = ThreadLocal.withInitial(OtherDataCollection::new);
    private static ThreadLocal<InternalDataCollection> internalThreadLocal = ThreadLocal.withInitial(InternalDataCollection::new);

    private static ThreadLocal<BaseDataCollection> useThradLocal = new ThreadLocal<>();

    public static BannerDataCollection getBannerDataCollection() {
        return bannerThreadLocal.get();
    }

    public static FeedDataCollection getFeedDataCollection() {
        return feedThreadLocal.get();
    }

    public static OtherDataCollection getOtherDataCollection() {
        return otherThreadLocal.get();
    }

    public static InternalDataCollection getInternalDataCollection() {
        return internalThreadLocal.get();
    }

    public static BaseDataCollection getDC() {
        return useThradLocal.get();
    }

    public static void setDC(BaseDataCollection dc) {
        useThradLocal.set(dc);
    }
}
