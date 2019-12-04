package simple.jobstream.mapreduce.common.vip;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.hbase.mapreduce.SimpleTotalOrderPartitioner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.process.frame.JobStreamRun;
import com.process.frame.base.InHdfsOutHFile;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.JobConfUtil;
import com.process.frame.util.VipcloudUtil;

/**
 * <p>Description: 将 XXXXObject 转为 HFile 文件 </p>  
 * @author qiuhongyang 2018年12月13日 下午2:21:29
 */
public class XXXXObject2HFile extends InHdfsOutHFile
{
	public String inputHdfsPath  = "";
	public String outputHdfsPath = "";
	public String tableName      = "";
	public String familyName     = "";
	public int HRegionCount      = 200;
	public boolean IsClear       = false;
	
	@Override
	public String getHdfsInput() {
		return inputHdfsPath;
	}

	@Override
	public String getHdfsOutput() {
		return outputHdfsPath;
	}

	public void pre(Job job)
	{
		job.setJobName(job.getConfiguration().get("jobName"));

		inputHdfsPath  = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
		tableName      = job.getConfiguration().get("tableName");
		familyName     = job.getConfiguration().get("familyName");
		HRegionCount   = Integer.valueOf(job.getConfiguration().get("HRegionCount"));
		
		if (job.getConfiguration().get("IsClear").equals("true")) 
		{
			IsClear = true;
		}
	}

	public void post(Job job)
	{
	}

	@SuppressWarnings("resource")
	public void SetMRInfo(Job job)
	{
		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.9f);
		System.out.println("******mapred.reduce.slowstart.completed.maps*******"
				+ job.getConfiguration().get("mapred.reduce.slowstart.completed.maps"));
		
		JobConfUtil.setTaskPerMapMemory(job, 8000);
		JobConfUtil.setTaskPerReduceMemory(job, 8000);
		
		job.setMapperClass(ProcessMap.class);
		job.setReducerClass(KeyValueSortReducer.class);
		job.setPartitionerClass(SimpleTotalOrderPartitioner.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(KeyValue.class);
		TextOutputFormat.setCompressOutput(job, false);

		job.setNumReduceTasks(0);
		
		if (IsClear) 
		{
			try
			{
				Configuration config = HBaseConfiguration.create();
				
				for (Entry<Object, Object> entry : JobStreamRun.getCustomProperties().entrySet())
				{
					config.set(entry.getKey().toString(), entry.getValue().toString());
				}

				HBaseAdmin admin = new HBaseAdmin(config);
				if (admin.tableExists(tableName))
				{
					admin.disableTable(tableName);
					admin.deleteTable(tableName);
				}

				HTableDescriptor tableDesc = new HTableDescriptor(tableName);
				HColumnDescriptor cd = new HColumnDescriptor(Bytes.toBytes(familyName));
				cd.setCompressionType(Algorithm.LZO);
				tableDesc.addFamily(cd);
				tableDesc.setMaxFileSize(1073741824L);
				admin.createTable(tableDesc, VipcloudUtil.GenMD5HashSpilteKeys(HRegionCount));

				HTable hTable;
				hTable = new HTable(config, Bytes.toBytes(tableName));
				HFileOutputFormat.configureIncrementalLoad(job, hTable);
			}catch (IOException e) {
				e.printStackTrace();
			}catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static class ProcessMap extends Mapper<Text, BytesWritable, ImmutableBytesWritable, KeyValue>
	{
		public String inputPath = "";
		public String fName = "";

		public void setup(Context context) throws IOException,
				InterruptedException
		{
			inputPath = VipcloudUtil.GetInputPath((FileSplit) context.getInputSplit());
			fName = context.getConfiguration().get("familyName");
		}

		public void cleanup(Context context) throws IOException,
				InterruptedException
		{
		}

		public void map(Text key, BytesWritable values, Context context)
				throws IOException, InterruptedException
		{
			context.getCounter("map", "input count").increment(1);
			
			XXXXObject xxxx = new XXXXObject();
			VipcloudUtil.DeserializeObject(values.getBytes(), xxxx);
			
			String keyId = VipcloudUtil.idToMD5Hash(key.toString());

			KeyValue kv = null;

			kv = new KeyValue(Bytes.toBytes(keyId), Bytes.toBytes(fName),
					Bytes.toBytes(fName + ":_keyid"), Bytes.toBytes(key.toString()));

			if (kv != null)
			{
				context.write(new ImmutableBytesWritable(Bytes.toBytes(keyId)), kv);
			}

			for (Map.Entry<String, String> updateItem : xxxx.data.entrySet())
			{
				kv = new KeyValue(Bytes.toBytes(keyId), Bytes.toBytes(fName),
						Bytes.toBytes(fName + ":" + updateItem.getKey()),
						Bytes.toBytes(updateItem.getValue()));

				if (kv != null)
				{
					context.write(new ImmutableBytesWritable(Bytes.toBytes(keyId)), kv);
				}
			}
		}
	}

	
}
