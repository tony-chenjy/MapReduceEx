package flow_sum;

import java.io.IOException;

public class Driver {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        args = new String[]{"src/main/resources/flow_sum/input_small",
                            "src/main/resources/flow_sum/output/sum",
                            "src/main/resources/flow_sum/output/sort",
                            "src/main/resources/flow_sum/output/province"};

        String[] sumPaths = new String[]{args[0], args[1]};
        String[] sumSortPaths = new String[]{args[1], args[2]};
        String[] provincePartitionPaths = new String[]{args[1], args[3]};

        FlowSumMapReduce.main(sumPaths);
        FlowSumSortMapReduce.main(sumSortPaths);
        FlowSumProvinceMapReduce.main(provincePartitionPaths);
    }
}
