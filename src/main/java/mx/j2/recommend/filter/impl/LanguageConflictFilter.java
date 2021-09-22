package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;

import java.util.*;

/**
 * 语言冲突过滤
 * <p>
 * 过滤掉在 language list 的冲突集合中，而不在 language list 中的视频
 */
@SuppressWarnings("unused")
public class LanguageConflictFilter extends BaseFilter {

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection baseDc) {
        // 如果参数有问题，不过滤
        if (!checkParams(doc.languageId, baseDc.req.languageList)) {
            return false;
        }

        // 请求语言列表
        List<String> reqLanguageListSource = baseDc.req.languageList;
        List<String> reqLanguageList = new ArrayList<>();
        for (String s : reqLanguageListSource) {
            reqLanguageList.add(DefineTool.Language.findLanaguage(s, DefineTool.Language.NoLanguage).id);
        }

        // 如果视频语言在请求语言列表里，不过滤
        if (reqLanguageList.contains(doc.languageId)) {
            return false;
        }

        // 遍历请求中的语言
        for (String language : reqLanguageList) {
            // 该语言对应的冲突集合
            Set<String> conflictSet = LANGUAGE_ID_TO_CONFLICTS_MAP.get(language);

            // 该语言有冲突集合且视频语言位于冲突集合中，过滤
            if (conflictSet != null && conflictSet.contains(doc.languageId)) {
                return true;
            }
        }

        return false;
    }

    /**
     * 检查参数是否对于过滤有效
     */
    private boolean checkParams(String docLanguageId, List<String> reqLanguageList) {
        // 如果请求没有携带语言列表，不过滤
        if (MXJudgeUtils.isEmpty(reqLanguageList)) {
            return false;
        }

        // 如果视频语言为空或者为 no_language，不过滤
        return MXStringUtils.isNotEmpty(docLanguageId)
                && !docLanguageId.equals(DefineTool.Language.NoLanguage.id);
    }

    /**
     * Tamil 语言的冲突集合
     */
    private static final Set<String> TAMIL_CONFLICT_SET = new HashSet<String>() {
        {
            add(DefineTool.Language.Telugu.id);
            add(DefineTool.Language.Kannada.id);
            add(DefineTool.Language.Gujarati.id);
            add(DefineTool.Language.Malayalam.id);
            add(DefineTool.Language.Bengali.id);
        }
    };

    /**
     * Telugu 语言的冲突集合
     */
    private static final Set<String> TELUGU_CONFLICT_SET = new HashSet<String>() {
        {
            add(DefineTool.Language.Tamil.id);
            add(DefineTool.Language.Kannada.id);
            add(DefineTool.Language.Gujarati.id);
            add(DefineTool.Language.Malayalam.id);
            add(DefineTool.Language.Bengali.id);
        }
    };

    /**
     * Marathi 语言的冲突集合
     */
    private static final Set<String> MARATHI_CONFLICT_SET = new HashSet<String>() {
        {
            add(DefineTool.Language.Tamil.id);
            add(DefineTool.Language.Telugu.id);
            add(DefineTool.Language.Gujarati.id);
            add(DefineTool.Language.Malayalam.id);
            add(DefineTool.Language.Bengali.id);
            add(DefineTool.Language.Kannada.id);
        }
    };

    /**
     * Gujarati 语言的冲突集合
     */
    private static final Set<String> GUJARATI_CONFLICT_SET = new HashSet<String>() {
        {
            add(DefineTool.Language.Tamil.id);
            add(DefineTool.Language.Telugu.id);
            add(DefineTool.Language.Marathi.id);
            add(DefineTool.Language.Malayalam.id);
            add(DefineTool.Language.Bengali.id);
            add(DefineTool.Language.Kannada.id);
        }
    };

    /**
     * Kannada 语言的冲突集合
     */
    private static final Set<String> KANNADA_CONFLICT_SET = new HashSet<String>() {
        {
            add(DefineTool.Language.Tamil.id);
            add(DefineTool.Language.Telugu.id);
            add(DefineTool.Language.Gujarati.id);
            add(DefineTool.Language.Malayalam.id);
            add(DefineTool.Language.Bengali.id);
            add(DefineTool.Language.Marathi.id);
        }
    };

    /**
     * Malayalam 语言的冲突集合
     */
    private static final Set<String> MALAYALAM_CONFLICT_SET = new HashSet<String>() {
        {
            add(DefineTool.Language.Marathi.id);
            add(DefineTool.Language.Telugu.id);
            add(DefineTool.Language.Gujarati.id);
            add(DefineTool.Language.Bengali.id);
            add(DefineTool.Language.Kannada.id);
        }
    };

    /**
     * Hindi / Punjabi / Bengali 语言的冲突集合
     */
    private static final Set<String> HPB_CONFLICT_SET = new HashSet<String>() {
        {
            add(DefineTool.Language.Tamil.id);
            add(DefineTool.Language.Marathi.id);
            add(DefineTool.Language.Telugu.id);
            add(DefineTool.Language.Gujarati.id);
            add(DefineTool.Language.Malayalam.id);
            add(DefineTool.Language.Kannada.id);
        }
    };

    /**
     * 语言 id 到其冲突集合的映射
     */
    private static final Map<String, Set<String>> LANGUAGE_ID_TO_CONFLICTS_MAP = new HashMap<String, Set<String>>() {
        {
            put(DefineTool.Language.Tamil.id, TAMIL_CONFLICT_SET);
            put(DefineTool.Language.Telugu.id, TELUGU_CONFLICT_SET);
            put(DefineTool.Language.Marathi.id, MARATHI_CONFLICT_SET);
            put(DefineTool.Language.Gujarati.id, GUJARATI_CONFLICT_SET);
            put(DefineTool.Language.Kannada.id, KANNADA_CONFLICT_SET);
            put(DefineTool.Language.Malayalam.id, MALAYALAM_CONFLICT_SET);
            put(DefineTool.Language.Hindi.id, HPB_CONFLICT_SET);
            put(DefineTool.Language.Punjabi.id, HPB_CONFLICT_SET);
            put(DefineTool.Language.Bengali.id, HPB_CONFLICT_SET);
        }
    };
}
