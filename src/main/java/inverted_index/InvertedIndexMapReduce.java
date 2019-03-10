package inverted_index;

import org.apache.hadoop.mapred.OutputCollector;

import java.io.IOException;
import java.util.*;

/**
 * @author cm
 * @date 2019/2/6 0006 21:34
 */
public class InvertedIndexMapReduce {

    public static class Map {
        public void map(String key, Document value, OutputCollector<String, Integer> output) throws IOException {
            // Write your code here
            // Output the results into output buffer.
            // Ps. output.collect(String key, int value);

            String content = value.content;
            String[] words = content.split("\\s+");
            for (String word : words) {
                output.collect(word, value.id);
            }
        }
    }

    public static class Reduce {
        public void reduce(String key, Iterator<Integer> values, OutputCollector<String, List<Integer>> output) throws IOException {
            // Write your code here
            // Output the results into output buffer.
            // Ps. output.collect(String key, List<Integer> value);

            Set<Integer> ids = new HashSet<>();
            while (values.hasNext()) {
                ids.add(values.next());
            }
            output.collect(key, new ArrayList(ids));
        }
    }
}
