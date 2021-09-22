package mx.j2.recommend.data_source;

import java.util.HashMap;
import java.util.Map;

/**
 * @author ：zhongrenli
 * @date ：Created in 2:30 下午 2020/10/12
 */
public class LanguageNameToIdDataSource extends BaseDataSource{

    private Map<String, String> languageMap;

    public LanguageNameToIdDataSource(){
        init();
    }

    private void init(){
        languageMap = new HashMap<>(16);
    }

    private void parseLanguageMap(){

    }

    public String getLanguageIdByName(String name) {
        return languageMap.getOrDefault(name, null);
    }
}
