package mx.j2.recommend.data_model.flow;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author ：zhongrenli
 * @date ：Created in 2:04 下午 2020/10/15
 */
public class UserSelectLanguageFlow {
    /**
     * 由数组中语言，组成的 string
     */
    private String languageListString;

    /**
     * 用户选择的语言，支持多种，目前只有一种
     */
    private List<String> UserSelectLanguageList;

    /**
     * 语言占位元素列表
     */
    private List<LanguageElement> elementList;

    /**
     * 占位语言 name Set
     */
    private Set<String> languageNameElementSet;

    /**
     * 占位语言 id Set
     */
    private Set<String> languageIdElementSet;

    /**
     * 内部类：语言元素
     */
    static class LanguageElement {
        private String languageName;

        private int position;

        private double percentage;

        public String getLanguageName() {
            return languageName;
        }

        public void setLanguageName(String languageName) {
            this.languageName = languageName;
        }

        public int getPosition() {
            return position;
        }

        public void setPosition(int position) {
            this.position = position;
        }

        public double getPercentage() {
            return percentage;
        }

        public void setPercentage(double percentage) {
            this.percentage = percentage;
        }
    }

    public String getLanguageListString() {
        return languageListString;
    }

    public void setLanguageListString(String languageListString) {
        this.languageListString = languageListString;
    }

    public List<String> getUserSelectLanguageList() {
        return UserSelectLanguageList;
    }

    public void setUserSelectLanguageList(List<String> userSelectLanguageList) {
        this.UserSelectLanguageList = userSelectLanguageList;
    }

    public List<LanguageElement> getElementList() {
        return elementList;
    }

    public void setElementList(List<LanguageElement> elementList) {
        this.elementList = elementList;
    }

    public void addElementToList(LanguageElement element) {
        if (null == this.elementList) {
            this.elementList = new ArrayList<>();
        }
        this.elementList.add(element);
    }

    public Set<String> getLanguageNameElementSet() {
        return languageNameElementSet;
    }

    public void setLanguageNameElementSet(Set<String> languageNameElementSet) {
        this.languageNameElementSet = languageNameElementSet;
    }

    public void addElementToSet(String name) {
        if (null == this.languageNameElementSet) {
            this.languageNameElementSet = new HashSet<>();
        }
        this.languageNameElementSet.add(name);
    }
}
