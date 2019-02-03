package sentiment_analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author cm
 * @date 2019/2/3 0003 10:04
 */
public class SentimentMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    public static final IntWritable ONE = new IntWritable(1);
    public static Map<String, String> dictionary = new HashMap<>();

    protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        String dictPath = context.getConfiguration().get("dictionary");
        BufferedReader br = new BufferedReader(new FileReader(dictPath));

        String line = br.readLine();
        while (line != null) {
            String[] words = line.split("\t");
            dictionary.put(words[0], words[1]);
            line = br.readLine();
        }
    }

    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        String line = value.toString();
        String[] words = line.split("\\s+");

        for (String word : words) {
            if (dictionary.containsKey(word)) {
                context.write(new Text(fileName + "\t" + dictionary.get(word)), ONE);
            }
        }
    }
}
