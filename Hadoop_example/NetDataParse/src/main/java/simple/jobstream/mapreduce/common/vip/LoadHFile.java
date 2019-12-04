package simple.jobstream.mapreduce.common.vip;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.log4j.Logger;

import com.process.frame.base.OnlyEmptyMapJobInfo;
import com.process.frame.util.EmptyInputFormat;

public class LoadHFile extends OnlyEmptyMapJobInfo
{
	private static Logger logger = Logger.getLogger(LoadHFile.class);
	
	public void pre(Job job)
	{
		job.setJobName(job.getConfiguration().get("jobName"));
	}

	public void SetMRInfo(Job job)
	{
		job.setMapperClass(ProcessMap.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setInputFormatClass(EmptyInputFormat.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		((JobConf) job.getConfiguration()).setNumMapTasks(1);
	}
	
	
	public static void HFileToHBase(String InputHFile, String TableName, @SuppressWarnings("rawtypes") Context context) throws Throwable
	{
//		Configuration config = HBaseConfiguration.create();
		Configuration config = context.getConfiguration();
		
		//设置1G的region大小
		config.setLong("hbase.hregion.max.filesize", 1073741824L);
		//config.set("zookeeper.znode.parent", "/mr_hbase");
         
		//将HFile载入HBase
		HTable table = new HTable(config, TableName);
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(config);
        loader.doBulkLoad(new Path(InputHFile), table);
	}
	

	public void post(Job job)
	{
	}

	public static class ProcessMap extends
			Mapper<IntWritable, IntWritable, IntWritable, NullWritable>
	{
		public String hfilePath = "";
		public String tableName = "";
		public String familyName = "";
		
		public void setup(Context context) throws IOException, InterruptedException
        {
			hfilePath = context.getConfiguration().get("hfilePath");
			tableName = context.getConfiguration().get("tableName");
			familyName = context.getConfiguration().get("familyName");
        }
	
		public void map(IntWritable key, IntWritable values, Context context)
				throws IOException, InterruptedException
		{
			try
			{
				HFileToHBase(hfilePath, tableName, context);
			}catch (Exception e) {
				logger.error("Error in buldload...");
				throw new IOException(e);
			} catch (Throwable e) {
				e.printStackTrace();
			}
		}
	}
}
