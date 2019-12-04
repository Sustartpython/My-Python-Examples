package simple.jobstream.mapreduce.user.walker.WOS;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.process.frame.JobStreamRun;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

public class MergeXXXXObject2Temp extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 100;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	
  
	public void pre(Job job)
	{
		String jobName = this.getClass().getSimpleName();
		
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
		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.7f);
		System.out.println("******mapred.reduce.slowstart.completed.maps*******" + job.getConfiguration().get("mapred.reduce.slowstart.completed.maps"));
		
		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(ProcessReducer.class);
		
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(BytesWritable.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(BytesWritable.class);
	    
	    //job.setInputFormatClass(SimpleTextInputFormat.class);
	    //job.setOutputFormatClass(TextOutputFormat.class);
	    
	    
		SequenceFileOutputFormat.setCompressOutput(job, false);
		job.setNumReduceTasks(reduceNum);
		
		try {
			Configuration conf = new Configuration();
			for (Entry<Object, Object> entry : JobStreamRun.getCustomProperties().entrySet())
			{
				conf.set(entry.getKey().toString(), entry.getValue().toString());
			}
			URI uri = URI.create(conf.get("fs.defaultFS"));
			FileSystem hdfs;
			hdfs = FileSystem.get(uri, conf);

			String dir = job.getConfiguration().get("newDataDir");
			Path path = new Path(dir);
			System.out.println("****** delete:" + path.getName());
			hdfs.delete(path, true);	//删除目录
			System.out.println("****** mkdirs:" + path.getName());
			hdfs.mkdirs(path);			//创建目录
			
			dir = job.getConfiguration().get("updateDataDir");
			path = new Path(dir);
			System.out.println("****** delete:" + path.getName());
			hdfs.delete(path, true);	//删除目录
			System.out.println("****** mkdirs:" + path.getName());
			hdfs.mkdirs(path);			//创建目录

			hdfs.close();
			
		} catch (Exception ex) {
			System.err.println("*****************exit****************:");
			ex.printStackTrace();
			System.exit(-1);
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
			Mapper<Text, BytesWritable, Text, BytesWritable> {
		
		public String inputPath = "";
		
		public void setup(Context context) throws IOException, InterruptedException
		{
			
			inputPath = VipcloudUtil.GetInputPath((FileSplit)context.getInputSplit());	//两级路径
		}
		
	    public void map(Text key, BytesWritable value, Context context
	                    ) throws IOException, InterruptedException {
	    	
	    	XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);

			
			String UT = "";
			String LIBName = "";		//SCI;SSCI;AHCI;ISTP;ISSHP;CCR;IC

			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("UT")) {
					UT = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("LIBName")) {
					LIBName = updateItem.getValue().trim();
				}
			}
			
			for (String lib : LIBName.split(";")) {
				context.getCounter("map", lib).increment(1);
			}

			if (!UT.startsWith("WOS:")) {
				context.getCounter("map", "error wos").increment(1);
				return;
			}
			
			
			if (inputPath.endsWith("/latest")) {		//累积老数据
				xObj.data.put("_NEW", "0");	 
				
	    		context.getCounter("map", "count latest").increment(1);
			}
	    	else {		//本趟新数据
	    		xObj.data.put("_NEW", "1");	 
	    		
	    		context.getCounter("map", "count newdata").increment(1);
			}
			
			byte[] outData = VipcloudUtil.SerializeObject(xObj);
			
			context.write(new Text(UT), new BytesWritable(outData));
		}
	}
	
	public static class ProcessReducer extends
  			Reducer<Text, BytesWritable, Text, BytesWritable> {
		private MultipleOutputs<Text, BytesWritable> mos;	
		private String newDataDir = null;
		private String updateDataDir = null;
		protected void setup(Context context)
				throws IOException, InterruptedException
		{
			mos = new MultipleOutputs<Text, BytesWritable>(context);
			newDataDir = context.getConfiguration().get("newDataDir");
			updateDataDir = context.getConfiguration().get("updateDataDir");
			
			System.out.println("newDataDir: " + newDataDir);
			System.out.println("updateDataDir: " + updateDataDir);
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			if(null != mos) {
				mos.close();
				mos = null;
	        }	
		}
		
	    public void reduce(Text key, Iterable<BytesWritable> values, 
	                       Context context
	                       ) throws IOException, InterruptedException {
	    	/**
	    	 * 如果大小差异大于指定百分比，取大值；否则，下载日期的新值；否则，取解析时间的新值
	    	 */
	    	
	    	float diffPercent = 0.3f;		//差距百分比
	    	HashSet<String> libSet = new HashSet<String>();	
	    	
	    	BytesWritable bwOut = new BytesWritable();	//用于最后输出
	    	BytesWritable bw1 = new BytesWritable();	//
	    	BytesWritable bw2 = new BytesWritable();	//
	    	String down_date1 = "";			//下载日期
	    	String down_date2 = "";			//下载日期
	    	String parse_time1 = "";		//解析时间
	    	String parse_time2 = "";		//解析时间
	    	float len1 = 0;
	    	float len2 = 0;
	    	
	    	boolean hasNewData = false;
	    	int cnt = 0;
	    	for (BytesWritable item : values) {
	    		cnt += 1;
	    		
	    		XXXXObject xObj = new XXXXObject();
				VipcloudUtil.DeserializeObject(item.getBytes(), xObj);		
				String down_date = "";
				String parse_time = "";
				String LIBName = "";
				for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
					if (updateItem.getKey().equals("DOWNDate")) {
						down_date = updateItem.getValue().trim();	
						context.getCounter("reduce", "DOWNDate_" + down_date).increment(1);	
					}
					else if (updateItem.getKey().equals("parse_time")) {
						parse_time = updateItem.getValue().trim();
						context.getCounter("reduce", "parse_time").increment(1);	
					}
					else if (updateItem.getKey().equals("LIBName")) {
						LIBName = updateItem.getValue().toUpperCase().trim();
					}
					else if (updateItem.getKey().equals("_NEW")) {
						if (updateItem.getValue().trim().equals("1")) {
							hasNewData = true;
						}
					}
				}
				
				for (String lib : LIBName.split(";")) {
					lib = lib.trim();
					if (lib.length() > 0) {
						libSet.add(lib);
					}
				}
				
				xObj.data.remove("_NEW");	//移除附加判断信息
	    		
	    		if (1 == cnt) {
	    			//bw1.set(item.getBytes(), 0, item.getLength());
	    			bw1 = new BytesWritable(VipcloudUtil.SerializeObject(xObj));
	    			
	    			len1 = bw1.getLength();
	    			down_date1 = down_date;
	    			parse_time1 = parse_time;
				}
	    		else if (2 == cnt) {
	    			//bw2.set(item.getBytes(), 0, item.getLength());
	    			bw2 = new BytesWritable(VipcloudUtil.SerializeObject(xObj));
	    			
	    			len2 = bw2.getLength();
	    			down_date2 = down_date;
	    			parse_time2 = parse_time;
				}
			}

	    	if (cnt < 2) {
	    		context.getCounter("reduce", "cnt<2").increment(1);	
	    		
	    		bwOut.set(bw1.getBytes(), 0, bw1.getLength());	    		
			}
	    	else if (cnt > 2) {
	    		//多于两个值，有问题
	    		context.getCounter("reduce", "cnt>2").increment(1);	
	    		return;
			}
	    	else {		//cnt==2
	    		context.getCounter("reduce", "cnt==2").increment(1);	    		
	    		
    			//如果差异大于 diffPercent，取大值
	    		if (Math.abs(len1 - len2) 
	    				> (diffPercent * Math.max(len1, len2))) {
	    			if (len1 > len2) {
						bwOut.set(bw1.getBytes(), 0, bw1.getLength());
					}
					else {
						bwOut.set(bw2.getBytes(), 0, bw2.getLength());
					}
				}
	    		else {						
					//取下载新值
					if (down_date1.compareTo(down_date2) > 0) {
	    	    		bwOut.set(bw1.getBytes(), 0, bw1.getLength());
	    			}
	    	    	else if (down_date1.compareTo(down_date2) < 0) {
	    	    		bwOut.set(bw2.getBytes(), 0, bw2.getLength());
	    			}
					else {		//下载时间相同时，取解析新值
						if (parse_time1.compareTo(parse_time2) > 0) {
		    	    		bwOut.set(bw1.getBytes(), 0, bw1.getLength());
		    			}
						else {
							bwOut.set(bw2.getBytes(), 0, bw2.getLength());
						}
					}
				}
			}
	    	
	    	String libs = "";
			for (String lib : libSet) {
				if (libs.length() > 0) {
					libs += ";";
				}				
				libs += lib;
			}
	    	
	    	XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(bwOut.getBytes(), xObj);
			xObj.data.put("LIBName", libs);			//插入合并后的LIBName
			byte[] outData = VipcloudUtil.SerializeObject(xObj);
			context.getCounter("reduce", "out").increment(1);	
			context.write(key, new BytesWritable(outData));
			
			if (hasNewData && (cnt == 1)) {		//新数据
				context.getCounter("reduce", "_NewData").increment(1);	
    			mos.write(key, new BytesWritable(outData), newDataDir + "/new");    			
			}
			else if (cnt == 2) {	//更新数据
				context.getCounter("reduce", "_UpdateData").increment(1);	
    			mos.write(key, new BytesWritable(outData), updateDataDir + "/update");
			}
	    }
  }
}
