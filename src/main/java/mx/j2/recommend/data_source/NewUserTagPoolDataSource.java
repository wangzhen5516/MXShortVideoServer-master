package mx.j2.recommend.data_source;

import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.manager.MXDataSource;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class NewUserTagPoolDataSource extends BaseDataSource{

    private static final String requestUrlFormat = "/%s/_search?pretty=false";
    private static String LOCAL_CACHE_KEY = "tag_pool_lv3_video_list";
    private static String INDEX = "taka_flowpool_lv3_tag_v1";
    private static int NUM = 100;

    public NewUserTagPoolDataSource(){
        init();
    }

    public void init(){
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        service.scheduleAtFixedRate(this::setTagPoolCache,0,1, TimeUnit.MINUTES);
    }

    private void setTagPoolCache(){
        ElasticSearchDataSource dataSource = MXDataSource.ES();
        String req = String.format(requestUrlFormat,INDEX);
        String content = constructReq(NUM);
        BaseDataCollection.ESRequest esRequest = new BaseDataCollection.ESRequest(req,content,"NewUserLv3TagPoolRecall","","");
        List<BaseDocument> resList = dataSource.searchForDocuments(esRequest);
        MXDataSource.cache().setTagPoolLv3VideoListCache(LOCAL_CACHE_KEY,resList);
    }

    private String constructReq(int num){
        JSONObject content = new JSONObject();
        content.put("size",num);
        return content.toJSONString();
    }
}
