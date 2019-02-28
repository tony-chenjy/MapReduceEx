package flow_sum;

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
 * @author tony.chenjy
 * @date 2019-02-28
 */
public class Main {

    public static class FlowMapper extends Mapper<LongWritable, Text, Text, Text> {
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] array = line.split("\\s+");
            context.write(new Text(array[0]), new Text(array[6] + "/" + array[7]));
        }
    }

    public static class FlowReducer extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            int upSum = 0, downSum = 0;
            for (Text item : values) {
                String[] array = item.toString().trim().split("/");
                downSum += Integer.valueOf(array[0]);
                upSum += Integer.valueOf(array[1]);
            }
            context.write(key, new Text(downSum + "/" + upSum));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String inputPath = "src/main/resources/flow_sum/input";
        String outputPath = "src/main/resources/flow_sum/output";

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(flow_sum.Main.class);

        job.setMapperClass(flow_sum.Main.FlowMapper.class);
        job.setReducerClass(flow_sum.Main.FlowReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, new Path(outputPath + (int) (Math.random() * 1000)));

        job.waitForCompletion(true);
    }
}
