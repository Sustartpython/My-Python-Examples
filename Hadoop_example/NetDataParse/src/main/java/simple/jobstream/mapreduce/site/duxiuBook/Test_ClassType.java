package simple.jobstream.mapreduce.site.duxiuBook;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

public class Test_ClassType extends InHdfsOutHdfsJobInfo
{
	public String inputHdfsPath = "/vipuser/walker/output/FilterHdfsData";
	public String outputHdfsPath = "/vipuser/walker/output/Test_ClassType";

	public void pre(Job job) 
	{
		job.setJobName(this.getClass().getSimpleName());
	}

	public void post(Job job) 
	{
	}

	public String getHdfsInput() {
		return inputHdfsPath;
	}

	public String getHdfsOutput() {
		return outputHdfsPath;
	}
	public void SetMRInfo(Job job) 
	{
		job.setMapperClass(ProcessMap.class);
		//job.setCombinerClass(ProcessReducer.class);
		//job.setReducerClass(ProcessReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		//job.setInputFormatClass(SimpleTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setNumReduceTasks(0);
		//((JobConf) job.getConfiguration()).setNumMapTasks(10);
		TextOutputFormat.setCompressOutput(job, false);
	}

	// ======================================处理逻辑=======================================
	public static class ProcessMap extends	Mapper<Text, BytesWritable, Text, NullWritable> 
	{
		private static HashMap<String, String> FirstClassMap      = new HashMap<>();
		private static HashMap<String, String> SecondOnlyClassMap = new HashMap<>();
		private static HashMap<String, String> SecondClassMap     = new HashMap<>();		
		private static ClassType classtype = null;
		
		public void setup(Context context) throws IOException,InterruptedException 
		{
			String firstclass_info  = "/RawData/_rel_file/FirstClass.txt";
			String secondclass_info = "/RawData/_rel_file/SecondClass.txt";
			ClassLoad classload = new ClassLoad(context, firstclass_info, secondclass_info);
			
			FirstClassMap      = classload.getfirstclass();
			SecondOnlyClassMap = classload.getsecondonlyclass();
			SecondClassMap     = classload.getsecondclass();
			
			classtype = new ClassType(FirstClassMap, SecondClassMap, SecondOnlyClassMap);
		}
		
		public void map(Text key, BytesWritable values, Context context)
				throws IOException, InterruptedException 
		{
			context.getCounter("map", "input").increment(1);
			
			XXXXObject xxxobj = new XXXXObject();
			VipcloudUtil.DeserializeObject(values.getBytes(), xxxobj);
			
			String _keyid          = key.toString();
			String _class          = "";
			String classtypes      = "";
			String showclasstypes  = "";
			String type            = "";

			for (Map.Entry<String, String> updateItem : xxxobj.data.entrySet()) 
			{
				if (updateItem.getKey().equals("classtypes")) 
				{
					classtypes = updateItem.getValue().replaceAll("\r\n|\r|\n|\t","").trim();
				}
				if (updateItem.getKey().equals("showclasstypes")) 
				{
					showclasstypes = updateItem.getValue().replaceAll("\r\n|\r|\n|\t","").trim();
				}
				if (updateItem.getKey().equals("class")) 
				{
					_class = updateItem.getValue().replaceAll("\r\n|\r|\n|\t","").trim().toUpperCase();
				}
				if (updateItem.getKey().equals("type")) 
				{
					type = updateItem.getValue().replaceAll("\r\n|\r|\n|\t","").trim().toUpperCase();
				}
			}
			
			
			
			context.getCounter("map", "effct").increment(1);
			
			String T_classtypes     = classtype.GetClassTypes(_class);
			String T_showclasstypes = classtype.GetShowClassTypes(_class);
			
			if (classtypes.equals(T_classtypes)) {
				context.getCounter("map", "=classtypes").increment(1);
			}
			else {
				context.getCounter("map", "=!classtypes").increment(1);
			}
			
			if (showclasstypes.equals(T_showclasstypes)) {
				context.getCounter("map", "=showclasstypes").increment(1);
			}
			else {
				context.getCounter("map", "=!showclasstypes").increment(1);
			}
			
			/*
			if(!showclasstypes.equals(T_showclasstypes))
			{				
				context.getCounter("map", "output").increment(1);
				context.write(new Text(_keyid + "\t" + _class + "\t" + showclasstypes + "\r\n" + 
						_keyid + "\t" + _class + "\t" + T_showclasstypes), NullWritable.get());
			}
			*/
		}
	}

}
