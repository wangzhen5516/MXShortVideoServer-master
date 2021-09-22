package mx.j2.recommend.hystrix.es;

import com.alibaba.fastjson.JSONObject;
import com.netflix.hystrix.HystrixCommand;
import mx.j2.recommend.util.HystrixUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author ：zhongrenli
 * @date ：Created in 5:05 下午 2021/2/18
 */
public class ScrollRequestCommand extends HystrixCommand<List<JSONObject>> {
    private static final Logger log = LogManager.getLogger(ScrollRequestCommand.class);

    private SearchSourceBuilder content;
    private String index;
    private RestHighLevelClient restHighLevelClient;
    private long totalSize;

    private ScrollRequestCommand() {
        super(HystrixUtil.ES_SETTER);
    }

    public ScrollRequestCommand(RestHighLevelClient restHighLevelClient, String index, SearchSourceBuilder content, long totalSize) {
        this();
        this.index = index;
        this.content = content;
        this.totalSize = totalSize;
        this.restHighLevelClient = restHighLevelClient;
    }

    @Override
    protected List<JSONObject> run() throws Exception {
        List<JSONObject> objects = new ArrayList<>();
        long size = 0;
        Scroll scroll = new Scroll(TimeValue.timeValueMinutes(10L));
        SearchRequest searchRequest = new SearchRequest()
                .indices(index)
                .scroll(scroll)
                .source(content);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
        String scrollId = searchResponse.getScrollId();
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        List<JSONObject> temp = constructObject(searchHits);
        if (CollectionUtils.isEmpty(temp)) {
            return objects;
        }
        objects.addAll(temp);
        size += temp.size();
        if (size >= totalSize) {
            closeScroll(scrollId);
            return objects;
        }

        while (searchHits != null && searchHits.length > 0) {
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(scroll);
            try {
                searchResponse = restHighLevelClient.searchScroll(scrollRequest);
            } catch (IOException e) {
                e.printStackTrace();
            }
            scrollId = searchResponse.getScrollId();
            searchHits = searchResponse.getHits().getHits();
            if (searchHits != null && searchHits.length > 0) {
                List<JSONObject> t = constructObject(searchHits);
                if (CollectionUtils.isNotEmpty(t)) {
                    objects.addAll(t);
                    size += t.size();
                }
            }

            if (size >= totalSize) {
                break;
            }
        }
        closeScroll(scrollId);
        return objects;
    }

    private List<JSONObject> constructObject(SearchHit[] searchHits) {
        List<JSONObject> objects = new ArrayList<>();
        for (SearchHit searchHit : searchHits) {
            JSONObject o = new JSONObject();
            o.put("metadata_id", searchHit.getId());
            if (searchHit.getSourceAsMap().containsKey("publisher_id")) {
                o.put("publisher_id", searchHit.getSourceAsMap().get("publisher_id"));
            }
            objects.add(o);
        }
        return objects;
    }

    private boolean closeScroll(String scrollId) {
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        ClearScrollResponse clearScrollResponse = null;
        try {
            clearScrollResponse = restHighLevelClient.clearScroll(clearScrollRequest);
        } catch (IOException e) {
            log.error("clear-scroll-error: " + e);
        }
        assert clearScrollResponse != null;
        return clearScrollResponse.isSucceeded();
    }

    @Override
    protected List<JSONObject> getFallback() {
        HystrixUtil.logFallback(this);
        return null;
    }
}
