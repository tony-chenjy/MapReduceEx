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
public class FlowSumProvinceMapReduce {
    public static class FlowSumProvinceMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

        Text k = new Text();
        FlowBean v = new FlowBean();

        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
            //拿取一行文本转为String
            String line = value.toString();
            //按照分隔符 \t 进行分隔
            String[] fields = line.split("\t");
            //获取用户手机号
            String phoneNum = fields[0];

            long upFlow = Long.parseLong(fields[fields.length - 3]);
            long downFlow = Long.parseLong(fields[fields.length - 2]);

            k.set(phoneNum);
            v.setFlow(downFlow, upFlow);

            context.write(k, v);
        }
    }

    public static class FlowSumProvinceReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

        FlowBean v = new FlowBean();

        @Override
        protected void reduce(Text key, Iterable<FlowBean> flowBeans, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {

            long upFlowCount = 0;
            long downFlowCount = 0;

            for (FlowBean flowBean : flowBeans){
                upFlowCount += flowBean.getUpload_sum();
                downFlowCount += flowBean.getDownload_sum();
            }

            v.setFlow(downFlowCount, upFlowCount);
            context.write(key,v);
        }
    }


    public static void main(String[] args) throws Exception {
        String inputPath = "src/main/resources/flow_sum/input";
        String outputPath = "src/main/resources/flow_sum/output/province";

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //指定这个job所在的jar包位置
        job.setJarByClass(FlowSumProvinceMapReduce.class);

        //指定使用的Mapper是哪个类，Reducer是哪个类
        job.setMapperClass(FlowSumProvinceMapper.class);
        job.setReducerClass(FlowSumProvinceReducer.class);

        //设置业务逻辑Mapper类的输出key 和value的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //设置业务逻辑Reducer类的输出key 和value的数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        job.setNumReduceTasks(4);
        //设定ReduceTask数量 等于ProvincePartitioner中的provinceMap中数量
        // 分区个数 < ReduceTasks个数，正常执行，会有空结果文件产生
        // 分区个数 > ReduceTasks个数，错误 Illegal partition

        //这里指定自定义分区组件，如果不指定，默认就是hashcode
        job.setPartitionerClass(ProvincePartitioner.class);

        FileInputFormat.setInputPaths(job, inputPath); //输入数据
        //指定处理完成之后的结果保存位置
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        //向yarn集群提交这个job
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }
}
