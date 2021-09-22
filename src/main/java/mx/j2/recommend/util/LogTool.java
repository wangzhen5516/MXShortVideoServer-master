package mx.j2.recommend.util;

import com.alibaba.fastjson.JSON;
import com.newrelic.api.agent.NewRelic;
import mx.j2.recommend.data_model.document.ShortDocument;
import mx.j2.recommend.thrift.InternalRequest;
import mx.j2.recommend.thrift.Request;
import mx.j2.recommend.thrift.Response;
import mx.j2.recommend.thrift.UserInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Zhangxuejian
 */
public class LogTool {
    protected static final Logger log = LogManager.getLogger(LogTool.class);

    public static void printRequest(Request request) {
        String requestStr = "request : ";
        requestStr += "userId[" + request.getUserInfo().getUuid() + "] ";
        requestStr += "num[" + request.getNum() + "] ";
        requestStr += "type[" + request.getType() + "] ";
        requestStr += "interfaceName[" + request.getInterfaceName() + "] ";

        log.debug(requestStr);

    }

    public static void printResponse(Logger logger, Response response, String logId){
        Map<String, String> infoMap = new HashMap();
        infoMap.put("Response", response.toString());
        infoMap.put("ResponseSetRetry", String.valueOf(response.isNeedRetry()));
        infoMap.put("logId", logId);
        logger.info(JSON.toJSONString(infoMap));
    }

    public static void serializedRequest(Request request){
        UserInfo userInfo = new UserInfo();
        userInfo.setUserId(request.getUserInfo().getUserId()+"_press");
        userInfo.setUuid(request.getUserInfo().getUuid()+"_press");
        request.setUserInfo(userInfo);
        request.setExecTimeSign("32520973204000");
        log.info(Arrays.toString(ThriftSerialUtil.serialize(request)));
    }
    public static void printGenresRequest(Request request) {
        String requestStr = "Genresrequest : ";
        requestStr += "userId[" + request.getUserInfo().getUuid() + "] ";
    }

    /**
     *
     * 打印召回结果
     *
     */
    public static void printMergedList(List<ShortDocument> result) {
        if (MXJudgeUtils.isEmpty(result)) {
            log.debug("result is empty");
            return;
        }
        for (ShortDocument doc : result) {
            log.debug(String.format("result List [%s] [%s] [%s] [%s]",
                    String.valueOf(doc.id),
                    String.valueOf(doc.recallName),
                    doc.title));
        }
    }

    public static void printErrorLog(Logger logger, String errorNo, String errorMessage, Request request, Object... values) {
        Map errorMap = new HashMap();
        errorMap.put("error_no", errorNo);
        errorMap.put("error_message", errorMessage);
        if (null != request) {
            errorMap.put("request", request.toString());
            errorMap.put("logId", request.logId);
            errorMap.put("error_message", errorMessage);
            errorMap.put("attach", values);
            errorMap.put("userId", request.getUserInfo().getUserId());
            errorMap.put("uuId", request.getUserInfo().getUuid());
            errorMap.put("adId", request.getUserInfo().getAdId());
            errorMap.put("interfaceName", request.interfaceName);
        }
        logger.error(JSON.toJSONString(errorMap));
    }

    public static void printErrorLogForInternal(Logger logger, String errorNo, String errorMessage, InternalRequest request, Object... values) {
        Map errorMap = new HashMap();
        errorMap.put("error_no", errorNo);
        errorMap.put("error_message", errorMessage);
        if (null != request) {
            errorMap.put("request", request.toString());
            errorMap.put("error_message", errorMessage);
            errorMap.put("attach", values);
        }
        logger.error(JSON.toJSONString(errorMap));
    }

    public static void printJsonStatusLog(Logger logger, String message, String status) {
        if (MXJudgeUtils.isNotEmpty(message)) {
            Map messMap = new HashMap();
            messMap.put(message, status);
            NewRelic.noticeError(message);
            logger.error(JSON.toJSONString(messMap));
        }
    }

    /**
     * 仅用于调试打印，便于搜索删除
     */
    public static void debugDel(String tag, String message) {
        System.out.println(tag + "->" + message);
    }

    /**
     * 错误处理
     */
    public static void reportError(DefineTool.ErrorEnum errorEnum, Logger logger, Exception e) {
        if (errorEnum != null) {
            errorEnum.report(logger, e);
        }
    }
}