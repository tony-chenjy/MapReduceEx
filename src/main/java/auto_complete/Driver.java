package auto_complete;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @author tony.chenjy
 * @date 2019/2/9 0009 11:59
 */
public class Driver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String inputPath = args[0];
        String nGramLibPath = args[1] + (int) (Math.random() * 100);
        String numberOfGram = args[2];
        String threshold = args[3];  //the word with frequency under threshold will be discarded
        String numberOfFollowingWords = args[4];

        // job1: build ngram library
        Configuration conf1 = new Configuration();
        conf1.set("textinputformat.record.delimiter", ".");
        conf1.set("noGram", numberOfGram);

        Job job1 = Job.getInstance(conf1, "NGramLibraryBuilder");
        job1.setJarByClass(Driver.class);
        job1.setMapperClass(NGramLibraryBuilder.NGramMapper.class);
        job1.setReducerClass(NGramLibraryBuilder.NGramReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        TextInputFormat.setInputPaths(job1, inputPath);
        TextOutputFormat.setOutputPath(job1, new Path(nGramLibPath));
        job1.waitForCompletion(true);

        // job2: build language model
        Configuration conf2 = new Configuration();
        conf2.set("threshold", threshold);
        conf2.set("n", numberOfFollowingWords);
        DBConfiguration.configureDB(conf2, "com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/test", "root", "root");
        // DistributedCache.addLocalArchives(conf2, "src/main/resources/auto_complete/mysql-connector-java-5.1.39-bin.jar");

        Job job2 = Job.getInstance(conf2, "LanguageModel");
        job2.setJarByClass(Driver.class);
//        job2.addArchiveToClassPath(new Path("src/main/resources/auto_complete/mysql-connector-java-5.1.39-bin.jar"));
        job2.setMapperClass(LanguageModel.LanguageMapper.class);
        job2.setReducerClass(LanguageModel.LanguageReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(DBOutputWritable.class);
        job2.setOutputValueClass(NullWritable.class);

        TextInputFormat.setInputPaths(job2, nGramLibPath);
        DBOutputFormat.setOutput(job2, "output", new String[] {"starting_phrase", "following_word", "count"});
        job2.waitForCompletion(true);
    }
}
