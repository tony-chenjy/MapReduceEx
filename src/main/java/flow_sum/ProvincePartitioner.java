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
        provinceMap.put("137", 0); //模拟手机归属地
        provinceMap.put("138", 1);
        provinceMap.put("139", 2);
    }

    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        Integer code = provinceMap.get(text.toString().substring(0, 3));

        if (code != null) {
            return code;
        }

        return 3; //不在上方列表中的
    }
}
