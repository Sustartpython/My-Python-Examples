package simple.jobstream.mapreduce.user.qianjun;

import java.awt.List;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.BXXXXObject;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.SimpleTextInputFormat;
import com.process.frame.util.VipcloudUtil;


public class WordCount extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 1;

	private static int reduceNum = 1;
	
	public static final String inputHdfsPath = "/user/qianjun/wordcount/input";
	public static final String outputHdfsPath = "/user/qianjun/wordcount/output";
	

	
	public void pre(Job job) {
		String jobName = "WordCount";
		if (testRun) {
			jobName = "test_" + jobName;
		}
		
		job.setJobName(jobName);
	}

	public void post(Job job) {

	}

	public String getHdfsInput() {
		return inputHdfsPath;
	}

	public String getHdfsOutput() {
		return outputHdfsPath;
	}

	public void SetMRInfo(Job job) {
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(SimpleTextInputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(ProcessReducer.class);

		TextOutputFormat.setCompressOutput(job, false);

		if (testRun) {
			job.setNumReduceTasks(testReduceNum);
		} else {
			job.setNumReduceTasks(reduceNum);
		}
	}

	// ======================================处理逻辑=======================================
	public static class ProcessMapper extends
	Mapper<LongWritable, Text, Text, IntWritable>{
		//MapReduce需要继承自Mapper类，并在这个类中需要实现自定义方法map
		//LongWritable：输入的<key:value>的key值，此处为文本偏移量
		//Text：此处为输入的<key:value>的value值，此处一般为文本内容
		//Text：此处为输出的<key:value>的key值，此处为单词
		//IntWritable：此处为输出的<key:value>的value值，此处为单次数量，固定为1 ，IntWritable是对Integer进一步封装，使其可以进行序列化
		private final IntWritable one = new IntWritable(1); 
		//定义one的值恒为1
		
		
       
       
  
        public void map(LongWritable key, Text value, Context context)  
                throws IOException, InterruptedException {  
        	/*
        	String line = value.toString().toLowerCase();  
        	
            StringTokenizer token = new StringTokenizer(line);  
            while (token.hasMoreTokens()) {  
                word.set(token.nextToken());  
                context.write(word, one);  
            }  */
        	
        	String line  = value.toString();
        	String[] lintStrings = line.split(" ");
        	for (int i = 0; i < lintStrings.length; i++) {
//        		context.write(lintStrings[i], one);
        		context.write(new Text(lintStrings[i]), one);
			}
			
		}

	}

	public static class ProcessReducer extends
	Reducer<Text, IntWritable, Text, IntWritable> {  
		  
        public void reduce(Text key, Iterable<IntWritable> values,  
                Context context) throws IOException, InterruptedException {  
            int sum = 0;  
            for (IntWritable val : values) {  
                sum += val.get();  
            }  
            context.write(key, new IntWritable(sum));  
        }  

	}
}