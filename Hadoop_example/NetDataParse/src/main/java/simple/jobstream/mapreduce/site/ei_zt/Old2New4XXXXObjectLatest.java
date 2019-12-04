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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

//这个MR仅仅是拷贝作用
public class Old2New4XXXXObjectLatest extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 0;
	
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
		//job.setReducerClass(ProcessReducer.class);
		
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
		private static Map<String, String> mapString = new HashMap<String, String>();
		public void setup(Context context) throws IOException, InterruptedException
		{
			// 获取HDFS文件系统  
		    FileSystem fs = FileSystem.get(context.getConfiguration());
		
		    FSDataInputStream fin = fs.open(new Path("/user/xujiang/input/article.txt")); 
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
			byte[] inBytes = new byte[value.getLength()];  
			System.arraycopy(value.getBytes(), 0, inBytes, 0, value.getLength());  
			VipcloudUtil.DeserializeObject(inBytes, xObj);			
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("Accession number")) {
					AccessionNumber = updateItem.getValue().trim();
				}
			}

			if (mapString.containsKey(AccessionNumber)) {
				String docid = mapString.get(AccessionNumber);
				xObj.data.put("rawid", docid);
			}
	    	
	    	context.getCounter("map", "count").increment(1);
	    	
	    	byte[] outBytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(AccessionNumber), new BytesWritable(outBytes));


		}
	}
	
  public static class ProcessReducer extends
  			Reducer<Text, BytesWritable, Text, BytesWritable> {
	    public void reduce(Text key, Iterable<BytesWritable> values, 
	                       Context context
	                       ) throws IOException, InterruptedException {
	    	
	    	context.getCounter("reduce", "count").increment(1);
	    }
  }
}
