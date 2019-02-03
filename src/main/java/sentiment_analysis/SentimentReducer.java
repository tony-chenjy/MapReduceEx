package sentiment_analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author cm
 * @date 2019/2/3 0003 10:04
 */
public class SentimentReducer extends Reducer<Text, IntWritable, Text, LongWritable>{
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
//        Iterator i$ = values.iterator();
//
//        while(i$.hasNext()) {
//            VALUEIN value = i$.next();
//            context.write(key, value);
//        }
    }
}
