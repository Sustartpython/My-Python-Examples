package simple.jobstream.mapreduce;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import com.process.frame.base.InHbaseTableOutHdfsJobInfo;
import com.process.frame.util.VipcloudUtil;

public class VIPDataTest extends InHbaseTableOutHdfsJobInfo
{
	public static String outputTablePath = "/modify_title_info";
	public static String tableName = "modify_title_info";
	public static final String family = "main";

	// ==================================信息接口=====================================
	public void pre(Job job)
	{
		job.getConfiguration().setLong("mapreduce.job.counters.limit", 250);
	}

	public void post(Job job)
	{
	}

	public String GetHbaseInputTableName()
	{
		return tableName;
	}

	public Scan GetHbaseInputScan()
	{
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(family));
		scan.addColumn(Bytes.toBytes("main"), Bytes.toBytes("main:_keyid"));
		scan.addColumn(Bytes.toBytes("main"), Bytes.toBytes("main:language"));
		scan.setCaching(5000);
		return scan;
	}

	public String getHdfsOutput()
	{
		return outputTablePath;
	}

	public void SetMRInfo(Job job)
	{
		job.setOutputFormatClass(NullOutputFormat.class);
		job.setMapperClass(VIPDataTest.ProcessMap.class);
		job.setNumReduceTasks(0);
	}
	
	// ======================================处理逻辑=======================================
	public static class ProcessMap extends
			TableMapper<WritableComparable, Writable>
	{
		public void setup(Context context) throws IOException,
				InterruptedException
		{
			
		}

		public void cleanup(Context context) throws IOException,
				InterruptedException
		{
		
		}
		
		public void map(ImmutableBytesWritable key, Result values,
				Context context) throws IOException, InterruptedException
		{
			String language = VipcloudUtil.GetValue(values, "main", "language");
			if (language.equals("1"))
				context.getCounter("TitleInfo_chinese", "count").increment(1);
			else if (language.equals("2"))
				context.getCounter("TitleInfo_foreign", "count").increment(1);
		}
	}
}
