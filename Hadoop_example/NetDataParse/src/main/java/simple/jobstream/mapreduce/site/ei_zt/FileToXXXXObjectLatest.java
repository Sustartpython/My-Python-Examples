package simple.jobstream.mapreduce.site.ei_zt;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

public class FileToXXXXObjectLatest extends InHdfsOutHdfsJobInfo {
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
		private static Map<String, String> mapString = new HashMap<String, String>();
		
		public void setup(Context context) throws IOException, InterruptedException
		{
			
			inputPath = VipcloudUtil.GetInputPath((FileSplit)context.getInputSplit());	//两级路径
			
			// 获取HDFS文件系统  
		    FileSystem fs = FileSystem.get(context.getConfiguration());
		
		    FSDataInputStream fin = fs.open(new Path("/user/xujiang/input/article2.txt")); 
		    BufferedReader in = null;
		    String line;
		    try {
		        in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
		        while ((line = in.readLine()) != null) {
		        	line = line.trim();
		        	if (line.length() < 2) {
						continue;
					}
		        	String[] vec = line.split("\t");
		        	if (vec.length != 2) {
		        		continue;
					}
		        	
		        	mapString.put(vec[0], vec[1]);
		        }
		    } finally {
		        if (in!= null) {
		        	in.close();
		        }
		    }
		    System.out.println("mapString size:" + mapString.size());
		}
		
	    public void map(Text key, BytesWritable value, Context context
	                    ) throws IOException, InterruptedException {
	    	
	    	String AccessionNumber = "";
	    	
	    	XXXXObject xObj = new XXXXObject();
			byte[] bs = new byte[value.getLength()];  
			System.arraycopy(value.getBytes(), 0, bs, 0, value.getLength());  
			VipcloudUtil.DeserializeObject(bs, xObj);			
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("Accession number")) {
					AccessionNumber = updateItem.getValue().trim();
				}
			}

			if (mapString.containsKey(AccessionNumber)) {
				String docid = mapString.get(AccessionNumber);
				xObj.data.put("rawid", docid);
				context.getCounter("map", "add").increment(1);
			}
			
			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(key, new BytesWritable(bytes));

		}
	}
	
	public static class ProcessReducer extends
  			Reducer<Text, BytesWritable, Text, BytesWritable> {
	  	public void reduce(Text key, Iterable<BytesWritable> values, 
	                       Context context
	                       ) throws IOException, InterruptedException {
	    	/**
	    	 * 如果大小差异大于指定百分比，取大值；否则，下载日期的新值；否则，取解析时间的新值
	    	 */
	    	
	    	float diffPercent = 0.3f;		//差距百分比
	    	
	    	BytesWritable bwOut = new BytesWritable();	//用于最后输出
	    	BytesWritable bw1 = new BytesWritable();	//
	    	BytesWritable bw2 = new BytesWritable();	//
	    	String down_date1 = "";			//下载日期
	    	String down_date2 = "";			//下载日期
	    	String parse_time1 = "";		//解析时间
	    	String parse_time2 = "";		//解析时间
	    	float len1 = 0;
	    	float len2 = 0;
	    	
	    	int cnt = 0;
	    	for (BytesWritable item : values) {
	    		cnt += 1;
	    		
	    		XXXXObject xObj = new XXXXObject();
				VipcloudUtil.DeserializeObject(item.getBytes(), xObj);		
				String down_date = "";
				String parse_time = "";
				for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
					if (updateItem.getKey().equals("down_date")) {
						down_date = updateItem.getValue().trim();		
					}
					else if (updateItem.getKey().equals("parse_time")) {
						parse_time = updateItem.getValue().trim();
					}
				}
	    		
	    		if (1 == cnt) {
	    			bw1.set(item.getBytes(), 0, item.getLength());
	    			len1 = bw1.getLength();
	    			down_date1 = down_date;
	    			parse_time1 = parse_time;
				}
	    		else if (2 == cnt) {
	    			bw2.set(item.getBytes(), 0, item.getLength());
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
	    	
	    	context.getCounter("reduce", "count").increment(1);		
	    	bwOut.setCapacity(bwOut.getLength()); 	//将buffer设为实际长度
			context.write(key, bwOut);
	    }
	}
}
