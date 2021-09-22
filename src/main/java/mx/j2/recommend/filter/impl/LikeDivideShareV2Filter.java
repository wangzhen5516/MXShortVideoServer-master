package mx.j2.recommend.filter.impl;

/**
 * @author qiqi
 * @date 2021-02-05 11:01
 */
public class LikeDivideShareV2Filter extends LikeDivideShareFilter {

    @Override
    public String getCacheKey() {
        return this.getName();
    }
}
