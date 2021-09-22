package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.document.ShortDocument;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.data_source.PublisherPageCassandraDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.List;

/**
 * @author ：zhongrenli
 * @date ：Created in 2:39 下午 2021/3/22
 */
public class BaseVideosOfThePublisherFromCassandraRecall extends BaseRecall<BaseDataCollection> {

    private static final int RECALL_SIZE = 40;

    private static String FIRST_QUERY = "select * from publisher where publisher_id='%s' and is_private=%b limit %d;";

    private static String SECOND_QUERY = "select * from publisher where publisher_id='%s' and is_private=%b and (pin_time, is_ugc_content, online_time, video_id) < (%d, %b, %d, '%s') limit %d allow filtering;";

    @Override
    public void recall(BaseDataCollection dc) {
        if (MXStringUtils.isEmpty(dc.req.getResourceId())) {
            return;
        }

        if (MXStringUtils.isEmpty(dc.req.getResourceType())) {
            return;
        }

        //爬虫账号过滤
        if (dc.req.getResourceId().startsWith(DefineTool.CrawlerAccountEnum.CRAWLER_ACCOUNT.getPrefix())) {
            return;
        }
        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        String localCacheKey = String.format("%s_%s_%s", dc.req.getResourceId(), "First_page", this.getName());
        String query;
        if (StringUtils.isEmpty(dc.req.nextToken)) {
            List<BaseDocument> cacheDocumentList = localCacheDataSource.getPublisherPageCache(localCacheKey);
            if (CollectionUtils.isNotEmpty(cacheDocumentList)) {
                dc.mergedList.addAll(cacheDocumentList);
                return;
            }

            query = String.format(FIRST_QUERY, dc.req.getResourceId(), getPrivateField(), RECALL_SIZE);
        } else {
            JSONArray token = parseNextToken(dc.req.nextToken);
            if (null == token || token.size() < 5) {
                return;
            }
            boolean isUgcContent = Integer.parseInt(String.valueOf(token.get(2))) == 1;
            query = String.format(
                    SECOND_QUERY,
                    dc.req.getResourceId(),
                    getPrivateField(),
                    token.get(0),
                    isUgcContent,
                    token.get(3),
                    token.get(4),
                    RECALL_SIZE);
        }
        
        PublisherPageCassandraDataSource dataSource = MXDataSource.publisherPage();
        List<String> ids = dataSource.getVideosOfPublisher(query);
        List<BaseDocument> objects = MXDataSource.details().get(ids, this.getName());
        if (MXJudgeUtils.isNotEmpty(objects)) {
            dc.mergedList.addAll(objects);
        }
        if (StringUtils.isEmpty(dc.req.nextToken)) {
            localCacheDataSource.setPublisherPageCache(localCacheKey, dc.mergedList);
        }
    }

    private JSONArray parseNextToken(String nextToken) {
        if (MXJudgeUtils.isEmpty(nextToken)) {
            return null;
        }
        JSONArray result = new JSONArray();
        String[] tokens = MXStringUtils.split(nextToken, "|");
        if (5 > tokens.length) {
            return null;
        }

        int token0 = Integer.parseInt(tokens[0]);
        long token1 = Long.parseLong(tokens[1]);
        String token2 = tokens[2];
        int token3 = Integer.parseInt(tokens[3]);
        long token4 = Long.parseLong(tokens[4]);

        result.add(token4);
        result.add(token3);
        result.add(token0);
        result.add(token1);
        result.add(token2);

        return result;
    }

    boolean getPrivateField() {
        return false;
    }
}
