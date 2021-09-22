package mx.j2.recommend.adjuster.impl;

import junit.framework.TestCase;
import mx.j2.recommend.data_model.data_collection.FeedDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.document.ShortDocument;

import java.util.ArrayList;
import java.util.Random;

/**
 * @author ：zhongrenli
 * @date ：Created in 4:27 下午 2020/7/4
 */
public class AppNameShuffleAdjusterTest extends TestCase {

    private FeedDataCollection dc;

    public void setUp() throws Exception {
        dc = new FeedDataCollection();
        dc.mergedList = new ArrayList<>();

        Random random = new Random();
        for (int i = 0; i < 50; i++) {
            int num = random.nextInt(5);
            BaseDocument doc = new ShortDocument();
            doc.appName = String.valueOf(num);

            dc.mergedList.add(doc);
        }
    }

    public void testAdjust() {

    }
}