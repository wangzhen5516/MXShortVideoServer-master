package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.DocumentTag;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 孩子标签过滤
 * <p>
 * 过滤条件（同时满足1，2）：
 * 1 标签集合中有孩子标签：命中孩子标签集合且 confidence >= 50
 * 2 标签集合中有黄反标签：命中黄反标签集合且 confidence >= 50
 */
@SuppressWarnings("unused")
public class ChildTagFilter extends BaseFilter {

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection baseDc) {
        List<DocumentTag> tags = doc.getTags();

        // 标签列表不存在，不过滤
        if (MXJudgeUtils.isEmpty(tags)) {
            return false;
        }

        boolean hasChild = false;// 有孩子标签
        boolean hasSensitive = false;// 有黄反标签

        for (DocumentTag tag : tags) {
            if (isChild(tag)) {// 是孩子标签
                hasChild = true;

                if (hasSensitive) {// 已经有黄反标签
                    return true;// 过滤
                }

                continue;// 不可能既是孩子又是黄反的
            }

            if (isSensitive(tag)) {// 是黄反标签
                hasSensitive = true;

                if (hasChild) {// 已经有孩子标签
                    return true;// 过滤
                }
            }
        }

        return false;
    }

    /**
     * 是否是孩子
     */
    private boolean isChild(DocumentTag tag) {
        return tag.confidence >= 50
                && (CHILD_TAGS_SET.contains(tag.name)
                || CHILD_TAGS_SET.contains(tag.parentName));
    }

    /**
     * 是否是黄反的
     * 顶级标签和二级标签只要有一个能对上，就算匹配
     */
    private boolean isSensitive(DocumentTag tag) {
        return tag.confidence >= 50
                && (SENSITIVE_TAG_TOP_LEVEL_SET.contains(tag.parentName)
                || SENSITIVE_TAG_SECOND_LEVEL_SET.contains(tag.name));
    }

    /**
     * 孩子标签集合
     */
    private static final Set<String> CHILD_TAGS_SET = new HashSet<String>() {
        {
            add("Teen");
            add("Child");
            add("Baby");
            add("Baby Waking Up");
            add("Newborn");
            add("Kid");
            add("Student");
        }
    };

    /**
     * 黄反标签中的父标签集合
     */
    private static final Set<String> SENSITIVE_TAG_TOP_LEVEL_SET = new HashSet<String>() {
        {
            add("Explicit Nudity");
            add("Violence");
            add("Visually Disturbing");
        }
    };

    /**
     * 黄反标签中的子标签集合
     */
    private static final Set<String> SENSITIVE_TAG_SECOND_LEVEL_SET = new HashSet<String>(16, 1) {
        {
            // Explicit Nudity
            add("Nudity");
            add("Graphic Male Nudity");
            add("Graphic Female Nudity");
            add("Sexual Activity");
            add("Illustrated Nudity Or Sexual Activity");
            add("Adult Toys");

            // Violence
            add("Graphic Violence Or Gore");
            add("Physical Violence");
            add("Weapon Violence");
            add("Weapons");
            add("Self Injury");

            // Visually Disturbing
            add("Emaciated Bodies");
            add("Corpses");
            add("Hanging");
        }
    };
}
