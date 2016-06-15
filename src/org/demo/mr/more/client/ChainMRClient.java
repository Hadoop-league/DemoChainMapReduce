package org.demo.mr.more.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.demo.mr.more.core.FrequenciesMRCore;

/**
 * <p>
 * test main client.
 * </p>
 * Create Date: Jun 14, 2016
 * Last Modify: Jun 14, 2016
 * 
 * @author <a href="http://weibo.com/u/5131020927">Q-WHai</a>
 * @see <a href="http://blog.csdn.net/lemon_tree12138">http://blog.csdn.net/lemon_tree12138</a>
 * @version 0.0.1
 */
public class ChainMRClient {

    private final String firstInputPath = "/home/hadoop/temp/wordcount";
    private final String firstOutputPath = "/home/hadoop/temp/output/wordcount_tmp";
    private final String secondInputPath = firstOutputPath;
    private final String secondOutputPath = "/home/hadoop/temp/output/wordcount";
    
    public static void main(String[] args) throws Exception {
        ChainMRClient client = new ChainMRClient();
        client.execute();
    }
    
    private void execute() throws Exception {
        runWordCountJob(firstInputPath, firstOutputPath);
        runFrequenciesJob(secondInputPath, secondOutputPath);
    }
    
    private int runWordCountJob(String inputPath, String outputPath) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJobName("WordCount-job");
        job.setJarByClass(FrequenciesMRCore.class);

        job.setMapperClass(FrequenciesMRCore.WordCountMapper.class);
        job.setCombinerClass(FrequenciesMRCore.WordCountReducer.class);
        job.setReducerClass(FrequenciesMRCore.WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
    
    private int runFrequenciesJob(String inputPath, String outputPath) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJobName("Frequencies-job");
        job.setJarByClass(FrequenciesMRCore.class);

        job.setMapperClass(FrequenciesMRCore.FrequenciesMapper.class);
        // job.setCombinerClass(FrequenciesMRCore.FrequenciesReducer.class);
        job.setReducerClass(FrequenciesMRCore.FrequenciesReducer.class);
        // job.setNumReduceTasks(0);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
