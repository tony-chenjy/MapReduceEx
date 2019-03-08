package flow_sum;

import java.io.IOException;

public class Driver {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        String inputPath = "src/main/resources/flow_sum/input";
        String outputSumPath = "src/main/resources/flow_sum/output/sum";
        String outputSortPath = "src/main/resources/flow_sum/output/sort";
        String outputProvicePath = "src/main/resources/flow_sum/output/province";

        FlowSumMapReduce.main(new String[]{inputPath, outputSumPath});
        FlowSumSortMapReduce.main(new String[]{outputSumPath, outputSortPath});
        FlowSumProvinceMapReduce.main(new String[]{outputSumPath, outputProvicePath});
    }
}
