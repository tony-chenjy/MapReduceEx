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
 * @date 2019-02-28
 */
public class FlowSumMapReduce {

    public static class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
        Text outputKey = new Text();
        FlowBean outputValue = new FlowBean();

        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
            // inputValue: 13726230503	00-FD-07-A4-72-B8:CMCC	120.196.100.82	i02.c.aliimg.com	24	27	2481	24681	200
            // outputKey: phone_number
            // outputValue: download_flow \t upload_flow

            String[] fields = value.toString().trim().split("\t");

            String phone_number = fields[0];
            String download_flow = fields[fields.length - 3];
            String upload_flow = fields[fields.length - 2];

            outputKey.set(phone_number);
            outputValue.setFlow(Long.parseLong(download_flow), Long.parseLong(upload_flow));
            context.write(outputKey, outputValue);
        }
    }

    public static class FlowSumReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
        FlowBean outputValue = new FlowBean();

        protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {
            // inputKey: phone_number
            // inputValue: download_flow\tupload_flow
            // outputKey: phone_number
            // outputValue: download_flow\tupload_flow\ttotal_flow

            long upload_flow = 0, download_flow = 0;
            for (FlowBean value : values) {
                download_flow += value.getDownload_sum();
                upload_flow += value.getUpload_sum();
            }
            outputValue.setFlow(download_flow, upload_flow);
            context.write(key, outputValue);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // args = new String[] {"src/main/resources/flow_sum/input_small", "src/main/resources/flow_sum/output/sum"};

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(FlowSumMapReduce.class);

        job.setMapperClass(FlowSumMapper.class);
        job.setReducerClass(FlowSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
