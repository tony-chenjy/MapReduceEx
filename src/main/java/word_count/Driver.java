package word_count;

import flow_sum.FlowSumMapReduce;
import flow_sum.FlowSumSortMapReduce;

import java.io.IOException;

public class Driver {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        args = new String[]{"src/main/resources/word_count/input_small", "src/main/resources/word_count/output"};

        WordCountMapReduce.main(args);
    }
}
