package mx.j2.recommend.data_model.flow;

import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.data_source.RecommendFlowDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.FileTool;
import mx.j2.recommend.util.LogTool;
import mx.j2.recommend.util.MXJudgeUtils;

import javax.annotation.concurrent.ThreadSafe;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 解析器版本 V2
 * 当使用这个版本时，配置文件已经拆分为多个接口文件了。
 * <p>
 * 这个解析器版本对应新的改动：
 * 接口配置文件分成 range 和 flow 两个部分，需要分别解析后再聚合。
 */
@ThreadSafe
public class RecommendFlowParserV2 {
    static final String KEY_RANGE = "range";
    static final String KEY_FLOW = "flow";
    private static final String RANGE_SEPARATOR = "-";
    private static final String EXTENSION_SEPARATOR = ".";

    /**
     * 解析推荐流 V2
     * 因为文件格式变了，使用新版解析器，并提供报错
     *
     * @param filePath 配置文件夹路径
     */
    public static void parseRecommendFlow(final String filePath,
                                          final Map<String, RecommendFlow> outNameToFlowMap,
                                          final Map<String, Map<Integer, RecommendFlow>> outRangeToFlowMap) {
        File confFile = new File(filePath);

        if (confFile.isDirectory()) {// 是配置文件夹
            File[] files = confFile.listFiles();
            if (MXJudgeUtils.isNotEmpty(files)) {
                for (File fileIt : files) {
                    parseRecommendFlow(fileIt.getPath(), outNameToFlowMap, outRangeToFlowMap);
                }
            }
        } else {// 解析接口配置文件
            try {
                parseRecommendFlowIf(filePath, outNameToFlowMap, outRangeToFlowMap);
            } catch (Exception e) {
                LogTool.reportError(DefineTool.ErrorEnum.FATAL, null, e);
            }
        }
    }

    /**
     * 接口配置文件解析
     *
     * @param filePath          接口配置文件路径
     * @param outNameToFlowMap  小流量名到实体的映射，输出参数
     * @param outRangeToFlowMap 小流量区间到实体的映射，输出参数
     */
    private static void parseRecommendFlowIf(final String filePath,
                                             final Map<String, RecommendFlow> outNameToFlowMap,
                                             final Map<String, Map<Integer, RecommendFlow>> outRangeToFlowMap) throws Exception {
        String content = FileTool.readContent(filePath);
        JSONObject confRoot = JSONObject.parseObject(content);

        // 根据文件名解析出接口名
        String fileName = new File(filePath).getName();
        String interfaceName = fileName.substring(0, fileName.lastIndexOf(EXTENSION_SEPARATOR));

        // 解析该接口下所有的 flow
        Map<String, RecommendFlow> flowMapIf = parseFlow(interfaceName, confRoot.getJSONObject(KEY_FLOW));

        // 将该接口下的所有映射加入到总映射表
        outNameToFlowMap.putAll(flowMapIf);

        // 解析该接口下所有的 range
        Map<String, List<Range>> rangeMapIf = parseRange(confRoot.getJSONObject(KEY_RANGE));

        // 将 range 赋值给具体的 flow
        boundRangeToFlow(flowMapIf, rangeMapIf);

        // 构建区间到 flow 的映射
        outRangeToFlowMap.put(interfaceName, buildRangeToFlowMap(interfaceName, flowMapIf));
    }

    /**
     * 为 flow 设置 range 信息
     *
     * @param flowMap  小流量名到实体的映射
     * @param rangeMap 小流量名到区间列表的映射
     */
    static void boundRangeToFlow(final Map<String, RecommendFlow> flowMap,
                                 final Map<String, List<Range>> rangeMap) {
        List<Range> rangeListIt;
        JSONObject toStrIt;

        for (RecommendFlow flowIt : flowMap.values()) {
            // 为推荐流设置区间列表
            rangeListIt = rangeMap.get(flowIt.name);

            if (MXJudgeUtils.isNotEmpty(rangeListIt)) {
                flowIt.rangeList.addAll(rangeListIt);

                // 推荐流 log 插入 range
                toStrIt = JSONObject.parseObject(flowIt.toString);
                toStrIt.put(KEY_RANGE, rangeListIt.toString());
                flowIt.toString = toStrIt.toString();
            }
        }
    }

    /**
     * 解析区间配置
     *
     * @param rangeRoot root object
     * @return <SmallFlowName, List<Range>>
     */
    static Map<String, List<Range>> parseRange(final JSONObject rangeRoot) {
        Map<String, List<Range>> flowToRangeListMap = new HashMap<>();
        List<Range> rangeListIt;
        String flowNameIt;

        for (String rangeStrIt : rangeRoot.keySet()) {
            String[] tmp = rangeStrIt.split(RANGE_SEPARATOR);
            int rangeStart = Integer.parseInt(tmp[0]);
            int rangeEnd = Integer.parseInt(tmp[1]);

            // 区间合法值检查
            if (rangeStart < 0 || rangeStart >= RecommendFlowDataSource.SMALL_FLOW_NAME_BASE_NUMBER
                    || rangeEnd < 0 || rangeEnd >= RecommendFlowDataSource.SMALL_FLOW_NAME_BASE_NUMBER) {
                throw new IllegalArgumentException("Illegal flow range " + new Range(rangeStart, rangeEnd).toString());
            }

            flowNameIt = rangeRoot.getString(rangeStrIt);
            rangeListIt = flowToRangeListMap.computeIfAbsent(flowNameIt, s -> new ArrayList<>());
            rangeListIt.add(new Range(rangeStart, rangeEnd));
        }

        return flowToRangeListMap;
    }

    /**
     * 构建区间到流的映射
     *
     * @param interfaceName 接口名
     * @param nameToFlowMap 小流量名到实体的映射
     */
    public static Map<Integer, RecommendFlow> buildRangeToFlowMap(final String interfaceName,
                                                                  final Map<String, RecommendFlow> nameToFlowMap) throws Exception {
        // 该接口下的<区间槽位-流>表，满 10000 位
        Map<Integer, RecommendFlow> rangeSlotToFlowMap = new HashMap<>(RecommendFlowDataSource.SMALL_FLOW_NAME_BASE_NUMBER);

        for (RecommendFlow flowIt : nameToFlowMap.values()) {
            // 构建区间槽位号到流的映射
            buildRangeSlotToFlowMap(flowIt, rangeSlotToFlowMap);
        }

        // 映射数量和区间总数对不上，要报错
        if (rangeSlotToFlowMap.size() != RecommendFlowDataSource.SMALL_FLOW_NAME_BASE_NUMBER) {
            String message = "Interface " + interfaceName + "'s range total size less than " + RecommendFlowDataSource.SMALL_FLOW_NAME_BASE_NUMBER;
            throw new Exception(message);
        }

        return rangeSlotToFlowMap;
    }

    /**
     * 构建区间到推荐流的映射，每个槽位（即每个数字）都要建立映射关系
     *
     * @param recommendFlow         推荐流
     * @param outRangeSlotToFlowMap 映射表，输出参数
     */
    private static void buildRangeSlotToFlowMap(final RecommendFlow recommendFlow,
                                                final Map<Integer, RecommendFlow> outRangeSlotToFlowMap) throws Exception {
        RecommendFlow existFlow;// 该槽位上已映射的推荐流

        for (Range range : recommendFlow.rangeList) {// 每个区间
            for (int i = range.start; i <= range.end; i++) {// 每个槽位
                existFlow = outRangeSlotToFlowMap.putIfAbsent(i, recommendFlow);

                // 如果之前已经配置过了，说明配置重叠了，要报错
                if (existFlow != null) {
                    throw new Exception("Range slot " + i + " already has mapping small flow " + existFlow.name);
                }
            }
        }
    }

    /**
     * 解析该接口下所有的 flow
     *
     * @param interfaceName 接口名
     * @param flowRoot      root object
     * @return 小流量名到实体的映射
     */
    static Map<String, RecommendFlow> parseFlow(final String interfaceName,
                                                final JSONObject flowRoot) {
        JSONObject flowObjectIt;// 流配置
        RecommendFlow flowIt;// 流实体
        Map<String, RecommendFlow> result = new HashMap<>();

        for (String flowNameIt : flowRoot.keySet()) {
            flowObjectIt = flowRoot.getJSONObject(flowNameIt);// 一个流的配置
            flowIt = RecommendFlowParser.parseOneFlow(flowObjectIt);// 解析成实体

            // 基本信息回填
            flowIt.interfaceName = interfaceName;
            flowIt.name = flowNameIt;

            result.put(flowNameIt, flowIt);
        }

        return result;
    }

    public static void main(String[] args) {
        Map<String, RecommendFlow> result = new HashMap<>();
        Map<String, Map<Integer, RecommendFlow>> rangeSlotToFlowMap = new HashMap<>();

        try {
            RecommendFlowParserV2.parseRecommendFlow("./conf/flows", result, rangeSlotToFlowMap);

//            String name = "real_time_action_version_1_0_by_publisher_start2";
//            Map<Integer, RecommendFlow> mapIf = rangeSlotToFlowMap.get("real_time_action_version_1_0");

//            for (int i = 5000; i <= 10000; i++) {
//                if (!mapIf.get(i).name.equals(name)) {
//                    System.out.println("error");
//                }
//            }


//            for (Map.Entry<String, RecommendFlow> entry : result.entrySet()) {
//                System.out.println(entry.getKey());
//                System.out.println(entry.getValue().rangeList);
//            }

            // 输出所有接口的槽位信息
//            for (Map.Entry<String, Map<Integer, RecommendFlow>> entry : rangeSlotToFlowMap.entrySet()) {
//                System.out.println("interface: " + entry.getKey());
//
//                for (Map.Entry<Integer, RecommendFlow> entry1 : entry.getValue().entrySet()) {
//                    System.out.println(entry1.getKey() + " : " + entry1.getValue().name);
//                }
//            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
