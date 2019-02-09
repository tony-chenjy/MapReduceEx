package auto_complete;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author tony.chenjy
 * @date 2019/2/9 0009 12:11
 */
public class NGramLibraryBuilder {

    public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private int noGram;

        protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            Configuration config = context.getConfiguration();
            noGram = config.getInt("noGram", 2);
        }

        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            // read sentence by sentence, replace the non alphabet char
            // split sentence into words
            // build ngram library by sliding window

            final IntWritable ONE = new IntWritable(1);
            String sentence = value.toString().trim().toLowerCase().replaceAll("[^a-z]+", " ");
            String[] words = sentence.split("\\s+");

            if (words.length < 2) {
                return;
            }

            StringBuilder phrase;
            for (int i = 0; i < words.length; i++) {
                phrase = new StringBuilder();
                phrase.append(words[i]);
                for (int n = 1; n < noGram && (i + n) < words.length; n++) {
                    phrase.append(" ").append(words[i + n]);
                    context.write(new Text(phrase.toString().trim()), ONE);
                }
            }
        }
    }

    public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }
            // TODO do some filter, record only more than x
            context.write(key, new IntWritable(count));
        }
    }
}
