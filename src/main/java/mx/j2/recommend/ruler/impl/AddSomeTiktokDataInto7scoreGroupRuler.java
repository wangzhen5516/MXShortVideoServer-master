package mx.j2.recommend.ruler.impl;

import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.thrift.Result;
import org.apache.commons.lang.math.JVMRandom;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 *  author: xuejian.zhang
 *  一个临时规则, 用6分的tiktok数据往7分的队列里面掺, 目前掺了30%比例.
 */

public class AddSomeTiktokDataInto7scoreGroupRuler extends BaseRuler<BaseDataCollection> {
    private static Logger log = LogManager.getLogger(AddSomeTiktokDataInto7scoreGroupRuler.class);

    @Override
    public boolean skip(BaseDataCollection data) {
        float a = Conf.getTiktokContentRatio();
        if (a >= 1 || a <= 0) {
            log.error("tiktok ratio config wrong parameter : " + String.valueOf(Conf.getTiktokContentRatio()));
            return true;
        }
        return false;
    }

    @Override
    public void rule(BaseDataCollection dc) {
        float a = Conf.getTiktokContentRatio();

        float x = a / (1 - a);
        float score8 = 0.0f;
        float score6 = 0.0f;

        List<Result> tiktok8scoreResults = new ArrayList<>();
        List<Result> other8scoreResults = new ArrayList<>();
        List<Result> tiktok6scoreResults = new ArrayList<>();
        List<Result> other6scoreResults = new ArrayList<>();

        Set<Float> scoreSet = dc.scoreToResultListMap.keySet();
        for (Float score : scoreSet){
            // 把7分内容归类
            if (7.9 < score && 8.1 > score) {
                score8 = score;
                List<Result> sevenScoreList = dc.scoreToResultListMap.get(score);
                for (Result r : sevenScoreList) {
                    if (r.isSetInternalUse() && r.internalUse.isSetAppName() &&
                        r.internalUse.getAppName().equals("tiktok")) {
                        tiktok8scoreResults.add(r);
                    } else {
                        other8scoreResults.add(r);
                    }
                }
                sevenScoreList.clear();
            } else if (5.9 < score && 6.1 > score) {
                score6 = score;
                List<Result> sixScoreList = dc.scoreToResultListMap.get(score);
                for (Result r : sixScoreList) {
                    if (r.isSetInternalUse() && r.internalUse.isSetAppName() &&
                            r.internalUse.getAppName().equals("tiktok")) {
                        tiktok6scoreResults.add(r);
                    } else {
                        other6scoreResults.add(r);
                    }
                }
                sixScoreList.clear();
            }
            continue;
        }

        if (tiktok8scoreResults.size() > x * other8scoreResults.size()) {
            log.debug("tiktok content number is enough");
            dc.scoreToResultListMap.put(score8, tiktok8scoreResults);
            dc.scoreToResultListMap.get(score8).addAll(other8scoreResults);
            dc.scoreToResultListMap.put(score6, other6scoreResults);
            dc.scoreToResultListMap.get(score6).addAll(tiktok6scoreResults);
            Collections.shuffle(dc.scoreToResultListMap.get(score8), new JVMRandom());
            Collections.shuffle(dc.scoreToResultListMap.get(score6), new JVMRandom());
        } else {
            int needTiktokNumber = Math.round(x * other8scoreResults.size() - tiktok8scoreResults.size());
            if (tiktok6scoreResults.size() > 1) {
                tiktok8scoreResults.addAll(tiktok6scoreResults.subList(0, Math.min(needTiktokNumber, tiktok6scoreResults.size() - 1)));
            }
            tiktok6scoreResults.removeAll(tiktok8scoreResults);
            dc.scoreToResultListMap.put(score8, tiktok8scoreResults);
            dc.scoreToResultListMap.get(score8).addAll(other8scoreResults);
            dc.scoreToResultListMap.put(score6, other6scoreResults);
            dc.scoreToResultListMap.get(score6).addAll(tiktok6scoreResults);
            Collections.shuffle(dc.scoreToResultListMap.get(score8), new JVMRandom());
            Collections.shuffle(dc.scoreToResultListMap.get(score6), new JVMRandom());
        }
    }
}
