package simple.jobstream.mapreduce.site.WOS;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

public class Map2XXXXObject extends InHdfsOutHdfsJobInfo {
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
		
		private static Map<String, String> utmap = new HashMap<String, String>();
		
		public void setup(Context context) throws IOException, InterruptedException
		{
			
			initArrayList(context);
		}
		
		private static void initArrayList(Context context) throws IOException {
			FileSystem fs = FileSystem.get(context.getConfiguration());

			FSDataInputStream fin = fs.open(new Path("/user/lqx/wosnum/wosuniv.txt"));

			BufferedReader reader = null;
			try {
				reader = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
				String temp;

				while ((temp = reader.readLine()) != null) {
					String[] vec = temp.split("\t");
					if (vec.length != 2) {
						continue;
					}
					if (utmap.containsKey(vec[0])) {
						String value = utmap.get(vec[0])+";"+vec[1];
						utmap.put(vec[0], value);
					}
					else {
						utmap.put(vec[0], vec[1]);
					}
				}

			} catch (IOException e) {
				// TODO Auto-generated catch block
			} finally {
				if (reader != null) {
					reader.close();
				}
			}
			System.out.println("*******utsetsize:" + utmap.size());

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
			
			if (!utmap.containsKey(UT)) {
				return;
			}

			byte[] outData = VipcloudUtil.SerializeObject(xObj);
			
			context.write(new Text(UT.substring(4)), new BytesWritable(outData));
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
	    	
	    	HashSet<String> libSet = new HashSet<String>();	
	    	
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
					}
					else if (updateItem.getKey().equals("parse_time")) {
						parse_time = updateItem.getValue().trim();
					}else if (updateItem.getKey().equals("LIBName")) {
						LIBName = updateItem.getValue().toUpperCase().trim();
					}
				}
				
				for (String lib : LIBName.split(";")) {
					lib = lib.trim();
					if (lib.length() > 0) {
						libSet.add(lib);
					}
				}
				bwOut.set(item.getBytes(), 0, item.getLength());
	    		
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
	    	context.getCounter("reduce", "count").increment(1);		
			context.write(key, new BytesWritable(outData));
	    }
	}
}
