package word_count;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author cm
 * @date 2019/2/2 0002 11:29
 */
public class WordCountMapReduce {

    public static class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
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

    public static class WordReducer extends Reducer<Text, IntWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            long count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }

            context.write(key, new LongWritable(count));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String inputPath = args[0]; // "src/main/resources/word_count/input";
        String outputPath = args[1]; // "src/main/resources/word_count/output";

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(WordCountMapReduce.class);

        job.setMapperClass(WordMapper.class);
        job.setReducerClass(WordReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);
    }
}
