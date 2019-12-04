package simple.jobstream.mapreduce.user.walker;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.process.frame.base.InHbaseTableOutHdfsJobInfo;
import com.process.frame.util.VipcloudUtil;


/**
 * 描述：将hbase上(modify_title_info表)的语料读入到hdfs上并进行(xml格式)处理 只使用map，输入为hbase，输出为hdfs
 * 
 * @author czjiang
 */
public class CountFromHBase extends InHbaseTableOutHdfsJobInfo {
	public String outputTablePath = "/vipuser/walker/output/CountFromHBase";
	public String tableName = "modify_title_info";
	
	private static boolean testRun = false;

	public void pre(Job job) {
		String jobName = "CountFromHBase";
		if (testRun) {
			jobName = "test_" + jobName;
		}
		
		job.setJobName(jobName);
		job.getConfiguration().setLong("mapreduce.job.counters.limit", 250);
		
		// outputTablePath =
		// job.getConfiguration().get("czjiang.outputTablePath");
		// tableName = job.getConfiguration().get("vipcloud.hbase.tableName");
	}

	public void post(Job job) {

	}

	public String GetHbaseInputTableName() {
		return tableName;
	}

	public Scan GetHbaseInputScan() {
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("main"));
		scan.addColumn(Bytes.toBytes("main"), Bytes.toBytes("main:_keyid"));
		//scan.addColumn(Bytes.toBytes("main"), Bytes.toBytes("main:bookid"));
		//scan.addColumn(Bytes.toBytes("main"), Bytes.toBytes("main:title_c"));
		//scan.addColumn(Bytes.toBytes("main"), Bytes.toBytes("main:keyword_c"));
		//scan.addColumn(Bytes.toBytes("main"), Bytes.toBytes("main:keyword_e"));
		//scan.addColumn(Bytes.toBytes("main"), Bytes.toBytes("main:remark_c"));
		//scan.addColumn(Bytes.toBytes("main"), Bytes.toBytes("main:media_c"));
		//scan.addColumn(Bytes.toBytes("main"), Bytes.toBytes("main:class"));
		//scan.addColumn(Bytes.toBytes("main"), Bytes.toBytes("main:classnum"));
		//scan.addColumn(Bytes.toBytes("main"), Bytes.toBytes("main:years"));
		//scan.addColumn(Bytes.toBytes("main"), Bytes.toBytes("main:language"));
		scan.addColumn(Bytes.toBytes("main"), Bytes.toBytes("main:type"));
		scan.addColumn(Bytes.toBytes("main"), Bytes.toBytes("main:libid"));
		scan.addColumn(Bytes.toBytes("main"), Bytes.toBytes("main:range"));
		
		/*
		scan.setCaching(5000);
		
		if (testRun) {
			SplitAlgorithm split = new RegionSplitter.HexStringSplit();
			byte[][] kks = split.split(5000);
			for (int i = 0; i < kks.length; i++)
			{
				byte[] bs = kks[i];
				if (i > 9)
				{
					scan.setStopRow(bs);
					break;
				}
			}
		}
		*/
		
		return scan;
	}

	public String GetHdfsOutputPath() {
		return outputTablePath;
	}
	
	public String getHdfsOutput() {
		return outputTablePath;
	}

	public void SetMRInfo(Job job) {
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setMapperClass(CountFromHBase.ProcessMap.class);
		TextOutputFormat.setCompressOutput(job, false);
		job.setNumReduceTasks(0);
	}

	// ======================================处理逻辑=======================================
	public static class ProcessMap extends TableMapper<Text, NullWritable> {
		public void map(ImmutableBytesWritable key, Result values,
				Context context) throws IOException, InterruptedException {
			String lngID = VipcloudUtil.GetValue(values, "main", "_keyid");
			//String bookid = VipcloudUtil.GetValue(values, "main", "bookid");
			//String title_c = VipcloudUtil.GetValue(values, "main", "title_c");
			//String keyword_c = VipcloudUtil.GetValue(values, "main",
			//		"keyword_c");
			//String keyword_e = VipcloudUtil.GetValue(values, "main",
			//		"keyword_e");
			//String remark_c = VipcloudUtil.GetValue(values, "main", "remark_c");
			//String media_c = VipcloudUtil.GetValue(values, "main", "media_c");
			//String years = VipcloudUtil.GetValue(values, "main", "years");
			//String language = VipcloudUtil.GetValue(values, "main", "language");
			String type = VipcloudUtil.GetValue(values, "main", "type");
			//String field = VipcloudUtil.GetValue(values, "main", "class");
			String libid = VipcloudUtil.GetValue(values, "main", "libid");
			String range = VipcloudUtil.GetValue(values, "main", "range");
			
			context.getCounter("map", "all").increment(1);
			
			if (type.equals("99")) {
				context.getCounter("map", "99").increment(1);
				return;
			}
			
			context.getCounter("map", "exclude 99").increment(1);
			
			if ((!libid.isEmpty()) && (!libid.toUpperCase().equals("VIP"))) {
				context.getCounter("map", "exclude libid").increment(1);
				return;
			}
			
			if (type.isEmpty() || type.equals("0")) {
				context.getCounter("map", "type null").increment(1);
				return;
			}
			
			HashSet<String> rangeSet = new HashSet<String>();
			String[] vec = range.toUpperCase().split(";");
			for (int i = 0; i < vec.length; i++) {
				rangeSet.add(vec[i].trim());
			}
			if (rangeSet.contains("SCI")) {
				context.getCounter("map", "range sci").increment(1);
			}
		}
	}


}