package mx.j2.recommend.task;

import com.alibaba.fastjson.JSONObject;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import mx.j2.recommend.util.MXStringUtils;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.*;

import static mx.j2.recommend.util.BaseMagicValueEnum.*;

/**
 * @author ：zhongrenli
 * @date ：Created in 4:14 下午 2020/11/6
 */
public class CassandraExecutor {
    private final ExecutorService workers;

    public CassandraExecutor(int threadNum) {
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("cassandra-%s").build();
        this.workers = Executors.newFixedThreadPool(threadNum, namedThreadFactory);
    }

    public void apply(String id,
                      String query,
                      Map<String, JSONObject> map,
                      CqlSession session,
                      CountDownLatch cd) {
        CompletableFuture<JSONObject> completableFuture = CompletableFuture.supplyAsync(() -> {
            ResultSet rs = session.execute(query);
            Iterator<Row> iterator = rs.iterator();

            JSONObject contentJSON = null;
            if (iterator.hasNext()) {
                Row row = iterator.next();
                String content = row.getString("content");
                contentJSON = JSONObject.parseObject(content);
                String mlTags = row.getString(ML_TAGS);
                String statistics = row.getString(STATISTICS);
                String feature30D = row.getString(FEATURE30D);
                String feature7D = row.getString(FEATURE7D);
                String feature1D = row.getString(FEATURE1D);
                float heatScore = row.getFloat(HEAT_SCORE);
                float heatScore2 = row.getFloat(HEAT_SCORE2);

                if (null != contentJSON) {
                    parseStringFromJsonObject(mlTags, ML_TAGS, contentJSON);
                    parseStringFromJsonObject(statistics, STATISTICS, contentJSON);
                    parseStringFromJsonObject(feature30D, FEATURE30D, contentJSON);
                    parseStringFromJsonObject(feature7D, FEATURE7D, contentJSON);
                    parseStringFromJsonObject(feature1D, FEATURE1D, contentJSON);

                    contentJSON.put(HEAT_SCORE, heatScore);
                    contentJSON.put(HEAT_SCORE2, heatScore2);
                }

            }
            return contentJSON;

        }, workers);

        completableFuture.whenComplete(((object, throwable) -> {
            try {
                map.put(id, object);
            } finally {
                cd.countDown();
            }
        }));
    }

    private void parseStringFromJsonObject(String objectString, String field, JSONObject json) {
        try {
            if (MXStringUtils.isNotEmpty(objectString)) {
                JSONObject object = JSONObject.parseObject(objectString);
                if (null != object) {
                    json.put(field, object);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
