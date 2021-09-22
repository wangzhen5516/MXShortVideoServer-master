package mx.j2.recommend.filter.impl;

public class ShareRateV6Filter extends ShareRateFilter {

    @Override
    public String getCacheKey() {
        return this.getName();
    }
}
