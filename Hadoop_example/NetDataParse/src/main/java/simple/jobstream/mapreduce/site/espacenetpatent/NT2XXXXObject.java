package simple.jobstream.mapreduce.site.espacenetpatent;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
//import org.apache.tools.ant.taskdefs.Length;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.BXXXXObject;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.SimpleTextInputFormat;
import com.process.frame.util.VipcloudUtil;

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class NT2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 40;
	private static int reduceNum = 40;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	//这个目录会被删除重建
  
	public void pre(Job job)
	{
		String jobName = "espacenetpatent." + this.getClass().getSimpleName();

		job.setJobName(jobName);
		
		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
	}
	
	public String getHdfsInput() {
		return inputHdfsPath;
	}

	public String getHdfsOutput() {
		return outputHdfsPath;
	}

	public void SetMRInfo(Job job)
	{
		job.getConfiguration().set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println(job.getConfiguration().get("io.compression.codecs"));

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(ProcessReducer.class);
		
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(BytesWritable.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(BytesWritable.class);
	    
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);

	    
		SequenceFileOutputFormat.setCompressOutput(job, false);
		if (testRun) {
			job.setNumReduceTasks(testReduceNum);
		} else {
			job.setNumReduceTasks(reduceNum);
		}
	}

	public void post(Job job)
	{
	
	}

	public String GetHdfsInputPath()
	{
		return inputHdfsPath;
	}

	public String GetHdfsOutputPath()
	{
		return outputHdfsPath;
	}

	
	public static class ProcessMapper extends 
			Mapper<LongWritable, Text, Text, BytesWritable> {
		//记录日志到HDFS
		public boolean log2HDFSForMapper(Context context, String text) {
			Date dt=new Date();//如果不需要格式,可直接用dt,dt就是当前系统时间
			DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");//设置显示格式
			String nowTime = df.format(dt);//用DateFormat的format()方法在dt中获取并以yyyy/MM/dd HH:mm:ss格式显示
			
			df = new SimpleDateFormat("yyyyMMdd");//设置显示格式
			String nowDate = df.format(dt);//用DateFormat的format()方法在dt中获取并以yyyy/MM/dd HH:mm:ss格式显示
			
			text = nowTime + "\n" + text + "\n\n";
			
			boolean bException = false;
			BufferedWriter out = null;
			try {
				// 获取HDFS文件系统  
		        FileSystem fs = FileSystem.get(context.getConfiguration());
		  
		        FSDataOutputStream fout = null;
		        String pathfile = "/RawData/epregister/log/" + nowDate + ".txt";
		        // 判断是否存在日志目录
		        if (fs.exists(new Path(pathfile))) {
		        	fout = fs.append(new Path(pathfile));
				}
		        else {
		        	fout = fs.create(new Path(pathfile));
		        }
		        
		        out = new BufferedWriter(new OutputStreamWriter(fout, "UTF-8"));
			    out.write(text);
			    out.close();
			    
			} catch (Exception ex) {
				bException = true;
			}
			
			if (bException) {
				return false;
			}
			else {
				return true;
			}
		}
		static int cnt = 0;	
		public static HashMap<String,String> map = new HashMap<String,String>();
		
		
		public static HashMap<String,String> parseHtml(Context context, String htmlText){
			System.out.println("language:"+htmlText);
			//初始化数据
			String Publication_Number = "";    //公开号
			String application_number  = "";   //申请号
			String CPC = "";                   //CPC分类号
			String IPC = "";                   //IPC分类号
			
			//解析数据
			Pattern pattern = Pattern.compile("/publication/(.*?)>");
			Matcher matcher = pattern.matcher(htmlText);
			if(matcher.find()){
				Publication_Number = matcher.group(1).replace("/","").replace("-","").trim();
			}
			if (Publication_Number.contains(".html")) {
				Publication_Number=Publication_Number.substring(0, Publication_Number.length()-9);
			}
			if (Publication_Number.contains(".pdf") || Publication_Number.contains(".xml")) {
				Publication_Number=Publication_Number.substring(0, Publication_Number.length()-8);
			}
			
			pattern = Pattern.compile("/application/.*?/(\\d+?)/.*?>");
			matcher = pattern.matcher(htmlText);
			if(matcher.find()){
				application_number = matcher.group(1).replace("/","").replace(".html", "").replace("-","").trim();
			}
			
			pattern = Pattern.compile("/cpc/(.*?)>");
			matcher = pattern.matcher(htmlText);
			if(matcher.find()){
				CPC = matcher.group(1).replace("-","/").trim();
			}
			
			pattern = Pattern.compile("/ipc/(\\d?)>");
			matcher = pattern.matcher(htmlText);
			if(matcher.find()){
				IPC = matcher.group(1).replace("-","/").trim();
			}
			
			
			System.out.println("Publication_Number:"+Publication_Number);
			System.out.println("application_number:"+application_number);
//			System.out.println("CPC:"+CPC);
//			System.out.println("IPC:"+IPC);
			
			
			
			//储存数据
			map.put("Publication_Number", Publication_Number);
			map.put("application_number", application_number);
			
			return map;

		}
		
	    public void map(LongWritable key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	    	cnt += 1;
	    	if (cnt == 1) {
				System.out.println("text:" + value.toString());
			}
	    	String text = value.toString().trim();
			
			HashMap<String,String> myMap = parseHtml(context, text);
	    	
//	    	if (myMap == null){
//	    		log2HDFSForMapper(context, "##" + value.toString() + "**");
//	    		context.getCounter("map", "null").increment(1);
//	    		return;
//	    	}
	    	
			XXXXObject xObj = new XXXXObject();
			xObj.data.put("Publication_Number", myMap.get("Publication_Number"));
			xObj.data.put("application_number", myMap.get("application_number"));
			byte[] bytes = com.process.frame.util.VipcloudUtil.SerializeObject(xObj);
			context.getCounter("map", "count").increment(1);
			context.write(new Text(myMap.get("Publication_Number") + "_" + myMap.get("application_number")), new BytesWritable(bytes));			
		}    
	}
	

	public static class ProcessReducer extends
		Reducer<Text, BytesWritable, Text, BytesWritable> {
		public void reduce(Text key, Iterable<BytesWritable> values, 
		                   Context context
		                   ) throws IOException, InterruptedException {
			
			
			BytesWritable bOut = new BytesWritable();	//用于最后输出
			for (BytesWritable item : values) {
				if (item.getLength() > bOut.getLength()) {	//选最大的一个
					bOut = item;
				}
			}			
			context.getCounter("reduce", "count").increment(1);
		
			context.write(key, bOut);
		}
	}
}
