package flow_sum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
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
public class FlowSumProvinceMapReduce {

    public static class FlowSumProvinceMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
        Text outputKey = new Text();
        FlowBean outputValue = new FlowBean();

        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
            // inputValue: 13726230503	14493	58893	73386
            // outputKey: phone_number
            // outputValue: download_flow\tupload_flow\ttotal_flow

            String[] phone_flows = value.toString().trim().split("\t");

            outputKey.set(phone_flows[0]);
            outputValue.setFlow(Long.parseLong(phone_flows[1]), Long.parseLong(phone_flows[2]));
            context.write(outputKey, outputValue);
        }
    }

    public static class FlowSumProvinceReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

        protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {
            // inputKey: phone_number
            // inputValue: <download_flow\tupload_flow\ttotal_flow, ...>
            // outputKey: phone_number
            // outputValue: download_flow\tupload_flow\ttotal_flow

            for (FlowBean value : values) {
                context.write(key, value);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // args = new String[]{"src/main/resources/flow_sum/output/sum", "src/main/resources/flow_sum/output/province"};

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(FlowSumProvinceMapReduce.class);

        job.setMapperClass(FlowSumProvinceMapper.class);
        job.setReducerClass(FlowSumProvinceReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        job.setNumReduceTasks(4);
        job.setPartitionerClass(ProvincePartitioner.class);

        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
