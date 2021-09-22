package mx.j2.recommend.data_model;

import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.interfaces.ICategoryShuffle;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.*;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/6/22 下午4:01
 * @description 简单的类别打散器，为简化处理，只按类别排一遍放在头部
 */
public class DocCategoryShuffleSimple implements ICategoryShuffle<BaseDocument> {

    @Override
    public void run(List<BaseDocument> list) {
        List<BaseDocument> categoryList = new ArrayList<>();
        Set<String> categorySet = new HashSet<>();
        Iterator<BaseDocument> it = list.iterator();
        BaseDocument docIt;
        String categoryIt;

        // 简化处理，只跑一遍按类别
        while (it.hasNext()) {
            docIt = it.next();
            categoryIt = getCategory(docIt);

            // 每个类别放一个
            if (categorySet.contains(categoryIt)) {
                continue;
            }

            categorySet.add(categoryIt);
            categoryList.add(docIt);
            it.remove();
        }

        // 类别列表插入头部
        list.addAll(0, categoryList);
    }

    @Override
    public String getCategory(BaseDocument document) {
        return MXJudgeUtils.isNotEmpty(document.categories) ? document.categories.get(0) : CATEGORY_DEFAULT;
    }
}
