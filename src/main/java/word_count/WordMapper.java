package word_count;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author cm
 * @date 2019/2/2 0002 15:11
 */
public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        final IntWritable ONE = new IntWritable(1);

        String line = value.toString();
        String[] words = line.split("\\s+");

        for (String word : words) {
            context.write(new Text(word), ONE);
        }
    }
}
