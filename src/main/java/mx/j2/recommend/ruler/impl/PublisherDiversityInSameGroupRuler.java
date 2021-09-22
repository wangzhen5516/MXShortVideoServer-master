package mx.j2.recommend.ruler.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.thrift.Result;
import org.apache.commons.lang.math.JVMRandom;

import java.util.*;

public class PublisherDiversityInSameGroupRuler extends BaseRuler<BaseDataCollection> {

    // 保证每N个里面, 不会有重复的publisher
    private static final int loopStep = 5;
    private static final int loopMaxLimit = 3;

    @Override
    public boolean skip(BaseDataCollection data) {
        return false;
    }

    @Override
    public void rule(BaseDataCollection dc) {
        for (float score : dc.scoreToResultListMap.keySet()) {
            List<Result> resultListBak = dc.scoreToResultListMap.get(score);
            List<Result> resultList = new ArrayList<>();

            Set<String> publisherPool = new HashSet<>();
            int before_proc_num = 1000;
            int after_proc_num = 0;
            int loopSizeDynamic = 0;
            List<Result> loopList = new ArrayList<>();

            // 先找到总共有多少个publisher_id, 第一把遍历的时候, 尽可能用充分
            for (int i = 0; i < resultListBak.size(); i++) {
                publisherPool.add(resultListBak.get(i).internalUse.publisherId);
            }

            while (loopSizeDynamic < publisherPool.size()) {
                loopSizeDynamic += loopStep;
            }
            loopSizeDynamic -= loopStep;

            publisherPool.clear();

            // 只要每次遍历都够装一袋的, 就继续
            int loop_i = 0;
            while (before_proc_num - after_proc_num >= loopSizeDynamic &&
                    loopSizeDynamic > 0 && loop_i < loopMaxLimit) {
                loop_i++;
                before_proc_num = resultListBak.size();

                for (int i = 0; i < resultListBak.size(); i++) {
                    Result r = resultListBak.get(i);
                    if (publisherPool.contains(r.internalUse.publisherId)) {
                        continue;
                    } else {
                        loopList.add(r);
                        publisherPool.add(r.internalUse.publisherId);
                    }
                    // 每装满一批, 就清算一批
                    if (loopSizeDynamic <= loopList.size()) {
                        resultList.addAll(loopList);
                        loopList.clear();
                        publisherPool.clear();
                    }
                }
                resultList.addAll(loopList);
                loopList.clear();
                publisherPool.clear();
                resultListBak.removeAll(resultList);
                after_proc_num = resultListBak.size();

                // 如果不够那么多了, 那就降低要求, 递减5个, 直到减为0为止, 跳出while
                while (before_proc_num - after_proc_num < loopSizeDynamic) {
                    loopSizeDynamic -= loopStep;
                }
            }
            // 把剩下的全部装在最后面
            Collections.shuffle(resultListBak, new JVMRandom());
            resultList.addAll(resultListBak);

            dc.scoreToResultListMap.put(score, resultList);
        }
    }
}
