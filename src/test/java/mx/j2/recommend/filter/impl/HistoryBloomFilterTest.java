//package mx.j2.recommend.filter.impl;
//
//import com.google.common.hash.BloomFilter;
//import mx.j2.recommend.conf.Conf;
//import mx.j2.recommend.data_model.data_collection.FeedDataCollection;
//import mx.j2.recommend.data_model.document.ShortDocument;
//import mx.j2.recommend.data_source.GuavaBloomDataSource;
//import mx.j2.recommend.thrift.Request;
//
//import java.util.ArrayList;
//import java.util.List;
//
///**
// * @author xiang.zhou
// * @description
// * @date 2020-07-07
// */
//public class HistoryBloomFilterTest  {
//
//    GuavaBloomDataSource bfds = new GuavaBloomDataSource();
//
//    public void test() {
//        Conf.loadConf("./conf/conf.sample.properties");
//
//        FeedDataCollection dc = new FeedDataCollection();
//        dc.req = new Request();
//        //dc.req.setUserInfo(new UserInfo("888888888888888", ""));
//
//        dc.mergedList = new ArrayList<>();
//        for(int i = 0; i < 1000; i++) {
//            ShortDocument document = new ShortDocument();
//            document.id = i+"";
//            dc.mergedList.add(document);
//        }
//
//        HistoryBloomFilter filter = new HistoryBloomFilter();
//        filter.filt(dc);
//    }
//
//    public void test_run(List<String> documents, String key) throws InterruptedException {
//        System.out.println("test2");
//        BloomFilter<String> bf = bfds.getBloomFilter(key);//TODO 如果没有的话  会超时，应该用exist来判断
//        for (String id : documents) {
//            if (bf.mightContain(id)) {
//                //System.out.println("exit");
//            }
//        }
//        if(bf.mightContain("something")) {
//            System.out.println("sp exit");
//        }
//        System.out.println("test");
//    }
//
//    public List<String> get_data() {
//        List<String> documents = new ArrayList<>();
//        for(int j = 0; j < 5000; j++) {
//            String item = "";
//            for(int i = 0; i < 100; i++) {
//                item += i-j;
//            }
//            documents.add(item);
//        }
//        documents.add("something");
//        return documents;
//    }
//
//    public void test_create() throws InterruptedException {
//        Conf.loadConf("./conf/conf.sample.properties");
//
//        GuavaBloomDataSource data = new GuavaBloomDataSource();
//        List<String> someIds = new ArrayList<>();
//        for(int i=0;i<100;i++) {
//            data.setHistoryToGuava(null, "zhou"+i, get_data());
//        }
//        System.out.println("success");
//    }
//
//    public void test_get() throws InterruptedException {
//        List<String> documents = get_data();
//        System.out.println("start");
//
//        for (int i = 0; i < 100; i++) {
//            new Thread(() -> {
//                try {
//                    test_run(documents, Thread.currentThread().getName());
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }, "zhou"+i).start();
//        }
//    }
//
//    public static void main(String[] args) throws InterruptedException {
//        HistoryBloomFilterTest test = new HistoryBloomFilterTest();
//        System.out.println("startting ");
//        test.test_get();
//    }
//
//}
