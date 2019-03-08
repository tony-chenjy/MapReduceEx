package flow_sum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

/**
 * @author tony.chenjy
 * @date 2019-03-08
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {
    public static Map<String, Integer> provinceMap = new HashMap();

    static {
        provinceMap.put("137", 1); //模拟手机归属地
        provinceMap.put("138", 2);
        provinceMap.put("139", 3);
    }

    @Override
    public int getPartition(Text text, FlowBean flowBean, int numTaskReduce) {
        String prefix = text.toString().substring(0, 3);
        Integer part = provinceMap.get(prefix);

        if (part == null) {
            return 0;
        }
        return part % numTaskReduce;
    }
}
