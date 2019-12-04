package simple.jobstream.mapreduce.user.walker.WOS;

import java.io.IOException;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.SimpleTextInputFormat;
import com.process.frame.util.VipcloudUtil;

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class Json2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 100;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	//这个目录会被删除重建
  
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
		job.getConfiguration().set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));
		
		
		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(ProcessReducer.class);
		
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(BytesWritable.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(BytesWritable.class);
	    
	    job.setInputFormatClass(SimpleTextInputFormat.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    
	    
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
			Mapper<LongWritable, Text, Text, BytesWritable> {
		
		static int cnt = 0;	
		
	    public void map(LongWritable key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	    	
	    	String line = value.toString().trim();
	    	
	    	cnt += 1;
	    	if (cnt == 1) {
				System.out.println("text:" + line);
			}
	    	
	    	Gson gson = new Gson();
			Type type = new TypeToken<Map<String, String>>(){}.getType();
			
			

			Map<String, String> mapField = gson.fromJson(line, type);		

			if (!mapField.containsKey("UT")) {
				context.getCounter("map", "no UT").increment(1);
				return;
			}
			if (!mapField.containsKey("TI")) {
				context.getCounter("map", "no TI").increment(1);
				return;
			}
			
			String UT = mapField.get("UT").trim();		//收录号
			String TI = mapField.get("TI").trim();		//标题 
			String LIBName = mapField.get("LIBName").trim();
			String DOWNDate = mapField.get("DOWNDate");
			
			context.getCounter("map", LIBName).increment(1);
			
			if (!UT.startsWith("WOS:")) {
				context.getCounter("map", "UT not startsWith WOS:").increment(1);
				return;
			}
			
			if (TI.length() < 1) {
				context.getCounter("map", "TI null").increment(1);
				return;
			}
			
			if (DOWNDate.length() != 8) {
				context.getCounter("map", "DOWNDate.length() != 8").increment(1);
				return;
			}
			
			XXXXObject xxxxObject = new XXXXObject();
			xxxxObject.data.put("parse_time", (new SimpleDateFormat("yyyy-MM-dd_kk:mm:ss")).format(new Date()));
			for (Map.Entry<String, String> entry : mapField.entrySet()) {
				xxxxObject.data.put(entry.getKey(), entry.getValue());
			}
			
			context.getCounter("map", "count").increment(1);			
			
			byte[] bytes = VipcloudUtil.SerializeObject(xxxxObject);
			context.write(new Text(UT), new BytesWritable(bytes));
			
		}
	}
	

	public static class ProcessReducer extends
		Reducer<Text, BytesWritable, Text, BytesWritable> {
		public void reduce(Text key, Iterable<BytesWritable> values, 
		                   Context context
		                   ) throws IOException, InterruptedException {
			
			HashSet<String> libSet = new HashSet<String>();	
			
			XXXXObject xObjOut = new XXXXObject();	//用于最后输出
			BytesWritable bwrite = new BytesWritable();
			for (BytesWritable item : values) {				
				XXXXObject xObj = new XXXXObject();
				byte[] bs = new byte[item.getLength()];  
				System.arraycopy(item.getBytes(), 0, bs, 0, item.getLength());  
				VipcloudUtil.DeserializeObject(bs, xObj);
				if (item.getLength() > bwrite.getLength()) {		//选最大一个
					bwrite.set(item.getBytes(), 0, item.getLength());
					xObjOut = xObj;
				}
				
				String LIBName = "";		//SCI;SSCI;AHCI;ISTP;ISSHP;ESCI;CCR;IC
		
				for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
					if (updateItem.getKey().equals("LIBName")) {
						LIBName = updateItem.getValue().toUpperCase().trim();
					}
				}
				
				for (String lib : LIBName.split(";")) {
					lib = lib.trim();
					if (lib.length() > 0) {
						libSet.add(lib);
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
			
			xObjOut.data.put("LIBName", libs);
			
			context.getCounter("reduce", "count").increment(1);
			
			byte[] outData = VipcloudUtil.SerializeObject(xObjOut);			
			context.write(key, new BytesWritable(outData));
		}
	}
}
