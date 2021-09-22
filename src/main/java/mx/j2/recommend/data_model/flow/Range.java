package mx.j2.recommend.data_model.flow;

/**
 * 小流量命中区间
 */
public class Range {
    public int start;
    public int end;

    Range(int start, int end) {
        this.start = start;
        this.end = end;
    }

    @Override
    public String toString() {
        return "[" + start + "-" + end + "]";
    }
}
