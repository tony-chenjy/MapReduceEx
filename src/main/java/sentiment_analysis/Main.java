package sentiment_analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author cm
 * @date 2019/2/3 0003 10:03
 */
public class Main {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String inputPath = "src/main/resources/sentiment_analysis/input";
        String outputPath = "src/main/resources/sentiment_analysis/output";
        String dictionaryPath = "src/main/resources/sentiment_analysis/emotionCategory.txt";

        Configuration conf = new Configuration();
        conf.set("dictionary", dictionaryPath);

        Job job = Job.getInstance(conf);

        job.setJarByClass(Main.class);

        job.setMapperClass(SentimentMapper.class);
        job.setReducerClass(SentimentReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath + (int)(Math.random() * 100)));

        job.waitForCompletion(true);
    }
}
