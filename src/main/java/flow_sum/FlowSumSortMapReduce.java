package flow_sum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
 * @date 2019-03-08
 */
public class FlowSumSortMapReduce {

    public static class FlowSumSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
        Text outputValue = new Text();
        FlowBean outputKey = new FlowBean();

        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowBean, Text>.Context context) throws IOException, InterruptedException {
            // input value : phone_number \t download_sum \t upload_sum \t total_sum
            // output value : download_sum\tupload_sum\ttotal_sum
            // output key : phone_number

            String[] fields = value.toString().trim().split("\t");
            String phoneNum = fields[0];

            long upFlow = Long.parseLong(fields[1]);
            long downFlow = Long.parseLong(fields[2]);

            outputKey.setFlow(upFlow,downFlow);
            outputValue.set(phoneNum);

            context.write(outputKey, outputValue);
        }
    }

    public static class FlowSumSortReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

        protected void reduce(FlowBean key, Iterable<Text> values, Reducer<FlowBean, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(value, key);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // args = new String[]{"src/main/resources/flow_sum/output/sum", "src/main/resources/flow_sum/output/sort"};

        org.apache.hadoop.conf.Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(FlowSumSortMapReduce.class);

        job.setMapperClass(FlowSumSortMapper.class);
        job.setReducerClass(FlowSumSortReducer.class);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
