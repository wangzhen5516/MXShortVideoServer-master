package mx.j2.recommend.util;

import mx.j2.recommend.thrift.Request;

import static mx.j2.recommend.util.DefineTool.FlowInterface.DEFAULT;

/**
 * @ Author     ：zhongrenli
 * @ Date       ：Created in 下午3:41 2019/1/2
 * @ Description：
 * @author zhongrenli
 */
public class InterfaceNameTransformateTool {

    private static final String MAIN_CLIENT_VERSION = "10000";


    public InterfaceNameTransformateTool(){

    }

    /**
     *
     * 替换interface name
     *
     */
    public void replaceInterfaceName(Request req){
        if (MXJudgeUtils.isEmpty(req.getInterfaceName())) {
            return;
        }
        req.setOriginalInterfaceName(req.getInterfaceName());
        DefineTool.FlowInterface flowInterface = DefineTool.FlowInterface.findFlowInterfaceByName(req.getInterfaceName(), DEFAULT);

        switch (flowInterface) {
            case MX_MAIN_VERSION_1_0:
                InterfaceNameTransformateTool.replace1_0Interface(req);
                break;
            case MX_MAIN_VERSION_2_0:
                InterfaceNameTransformateTool.replace2_0Interface(req);
                break;
            case MX_VIDEOS_OF_THE_PUBLISHER_VERSION_1_0:
                if(req.userInfo != null && req.userInfo.userId != null && req.userInfo.userId.equals(req.resourceId)) {
                    req.setInterfaceName(DefineTool.FlowInterface.MX_VIDEOS_OF_THE_PUBLISHER_ME_VERSION_1_0.getName());
                }
                break;
            default:
                break;
        }
    }

    /**
     *
     * 替换1_0interface name
     *
     */
    private static void replace1_0Interface (Request req) {
        DefineTool.TabInfoEnum tabInfoEnum = DefineTool.TabInfoEnum.findTabInfoEnumById(req.getTabId(),DefineTool.TabInfoEnum.DEFAULT);
        switch(tabInfoEnum){
            case HOT:
                req.setInterfaceName(DefineTool.FlowInterface.MX_HOT_TAB_INTERNAL_VERSION_1_0.getName());
                break;
            case STATUS:
                req.setInterfaceName(DefineTool.FlowInterface.MX_STATUS_TAB_INTERNAL_VERSION_1_0.getName());
                break;
            default:
                break;
        }
    }

    /**
     * 替换2_0interface name
     */
    private static void replace2_0Interface (Request req) {
        DefineTool.TabInfoEnum tabInfoEnum = DefineTool.TabInfoEnum.findTabInfoEnumById(req.getTabId(),DefineTool.TabInfoEnum.DEFAULT);
        switch(tabInfoEnum){
            case HOT:
                replaceHotTab(req);
                break;
            case STATUS:
                req.setInterfaceName(DefineTool.FlowInterface.MX_STATUS_TAB_INTERNAL_VERSION_1_0.getName());
                break;
            default:
                break;
        }
    }

    private static void replaceHotTab(Request req) {

        if (null == req.location || MXStringUtils.isBlank(req.location.country)) {
            if (MAIN_CLIENT_VERSION.equals(req.getClientVersion())) {
                req.setInterfaceName(DefineTool.FlowInterface.MX_HOT_TAB_INTERNAL_FOR_MAIN_VERSION_2_0.getName());
                return;
            }
            req.setInterfaceName(DefineTool.FlowInterface.MX_HOT_TAB_INTERNAL_VERSION_2_0.getName());
            return;
        }

        if (MAIN_CLIENT_VERSION.equals(req.getClientVersion())) {
            req.setInterfaceName(DefineTool.FlowInterface.MX_HOT_TAB_INTERNAL_FOR_MAIN_VERSION_2_0.getName());
        } else {
            req.setInterfaceName(DefineTool.FlowInterface.MX_HOT_TAB_INTERNAL_VERSION_2_0.getName());
        }
    }

}
