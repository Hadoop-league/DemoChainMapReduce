package org.demo.mr.more.core;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * <p>
 * chain map reduce core.
 * compute word frequencies.
 * </p>
 * Create Date: Jun 14, 2016
 * Last Modify: Jun 14, 2016
 * 
 * @author <a href="http://weibo.com/u/5131020927">Q-WHai</a>
 * @see <a href="http://blog.csdn.net/lemon_tree12138">http://blog.csdn.net/lemon_tree12138</a>
 * @version 0.0.1
 */
public class FrequenciesMRCore {

    public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
        private static final IntWritable one = new IntWritable(1);
        private static Text label = new Text();
        
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            while (tokenizer.hasMoreTokens()) {
                label.set(tokenizer.nextToken());
                context.write(label, one);
            }
        }
    }
    
    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable count = new IntWritable();
        
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            if (null == values) {
                return;
            }

            int sum = 0;
            for (IntWritable intWritable : values) {
                sum += intWritable.get();
            }
            count.set(sum);

            context.write(key, count);
        }
    }
    
    // KEYIN, VALUEIN, KEYOUT, VALUEOUT
    public static class FrequenciesMapper extends Mapper<Object, Text, IntWritable, Text> {
        
        //private IntWritable count = new IntWritable();
        
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, IntWritable, Text>.Context context)
                throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            String keyword =  tokenizer.nextToken();
            int wordCount = Integer.parseInt(tokenizer.nextToken());
            int riseCount = (wordCount + 10) - (wordCount + 10) % 10;
            
            Text valueText = new Text("[" + keyword + ":" + wordCount + "]");
            
            context.write(new IntWritable(riseCount), valueText);
        }
    }
    
    public static class FrequenciesReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values,
                Reducer<IntWritable, Text, IntWritable, Text>.Context context)
                        throws IOException, InterruptedException {
            
            System.out.println("sdfasdfasdfasdfdas");
            
            if (null == values) {
                return;
            }
            
            boolean firstFlag = true;
            StringBuffer buffer = new StringBuffer();
            for (Text text : values) {
                buffer.append((firstFlag ? "" : ", ") + text.toString());
                firstFlag = false;
            }
            
            System.out.println(buffer.toString());
            
            context.write(key, new Text(buffer.toString()));
        }
    }
}
