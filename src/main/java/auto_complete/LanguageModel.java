package auto_complete;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * @author tony.chenjy
 * @date 2019/2/9 0009 12:11
 */
public class LanguageModel {

    public static class LanguageMapper extends Mapper<LongWritable, Text, Text, Text> {
        private int threshold;

        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            this.threshold = conf.getInt("threshold", 0);
        }

        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            // read line by line
            // split into words
            // get the last word and count

            if (value == null || "".equals(value.toString().trim())) {
                return;
            }

            String line = value.toString().trim();
            String[] wordsPlusCount = line.split("\t");
            if (wordsPlusCount == null || wordsPlusCount.length < 2) {
                return;
            }

            String[] words = wordsPlusCount[0].split("\\s+");
            int count = Integer.valueOf(wordsPlusCount[1]);
            // filter the little phrase
            if (count < threshold) {
                return;
            }

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < words.length - 1; i++) {
                sb.append(words[i]).append(" ");
            }

            String outputKey = sb.toString().trim();
            String outputValue = words[words.length - 1];

            if (outputKey != null && outputKey.length() > 0) {
                context.write(new Text(outputKey), new Text(outputValue + "=" + count));
            }
        }
    }

    public static class LanguageReducer extends Reducer<Text, Text, DBOutputWritable, NullWritable> {
        private int n;

        protected void setup(Reducer<Text, Text, DBOutputWritable, NullWritable>.Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            this.n = conf.getInt("n", 5);
        }

        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, DBOutputWritable, NullWritable>.Context context) throws IOException, InterruptedException {

            // {50, [girls, boys]}, {60, [data]}
            Map<Integer, List<String>> map = new TreeMap<>();
            for (Text value : values) {
                String[] phrase = value.toString().trim().split("=");
                String followingWord = phrase[0].trim();
                int count = Integer.valueOf(phrase[1].trim());

                if (map.containsKey(count)) {
                    map.get(count).add(followingWord);
                } else {
                    List<String> list = new ArrayList<>();
                    list.add(followingWord);
                    map.put(count, list);
                }
            }
            // select top k
            Iterator<Integer> iter = map.keySet().iterator();
            for (int i = 0; i < n && iter.hasNext();) {
                int keyCount = iter.next();
                List<String> words = map.get(keyCount);
                for(String curWord: words) {
                    context.write(new DBOutputWritable(key.toString(), curWord, keyCount),NullWritable.get());
                    i++;
                }
            }
        }
    }
}
