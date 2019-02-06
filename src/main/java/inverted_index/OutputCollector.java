package inverted_index;

/**
 * @author cm
 * @date 2019/2/6 0006 21:35
 */
public abstract class OutputCollector {
    public abstract void collect(Object key, Object value);
}
