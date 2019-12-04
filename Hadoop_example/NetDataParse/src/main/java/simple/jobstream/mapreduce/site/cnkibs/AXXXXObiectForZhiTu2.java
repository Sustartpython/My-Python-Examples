package simple.jobstream.mapreduce.site.cnkibs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.cloudera.org.codehaus.jackson.map.util.Provider;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.vip.VipIdEncode;
import simple.jobstream.mapreduce.common.util.StringHelper;


// 将以往数据转为A层格式
public class AXXXXObiectForZhiTu2 extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 100;

	public static String inputHdfsPath = "/RawData/cnki/bs/latest";
	public static String outputHdfsPath = "/RawData/cnki/bs/latest_new";

	public void pre(Job job) {
		String jobName = "cnki_bs." + this.getClass().getSimpleName();
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

	public void SetMRInfo(Job job) {
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

	public void post(Job job) {

	}

	public String GetHdfsInputPath() {
		return inputHdfsPath;
	}

	public String GetHdfsOutputPath() {
		return outputHdfsPath;
	}

	public static class ProcessMapper extends Mapper<Text, BytesWritable, Text, BytesWritable> {
		// 老字段
		private static String dbcode="";
		private static String rawid="";
		private static String title="";
		private static String creator="";
		private static String creator_degree="";
		private static String creator_descipline="";
		private static String creator_institution="";
		private static String contributor="";
		private static String description="";
		private static String subject_clc="";
		private static String subject="";
		private static String date="";
		private static String description_fund="";
		private static String provider_url="";

		// 重新生成batch
		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
			{// 老字段

				rawid = "";
				title = "";
				subject_clc = "";
				subject = "";
				creator = "";
				creator_descipline = "";
				date = "";
				creator_degree = "";
				creator_institution = "";
				description = "";
				contributor = "";
				dbcode = "";
				description_fund = "";
				provider_url="";

			}

			XXXXObject xObj = new XXXXObject();
			byte[] bs = new byte[value.getLength()];  
			System.arraycopy(value.getBytes(), 0, bs, 0, value.getLength());  
			VipcloudUtil.DeserializeObject(bs, xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				}
				else if  (updateItem.getKey().equals("author")) {
					creator = updateItem.getValue().trim();
				}

				else if (updateItem.getKey().equals("degree")) {
					creator_degree = updateItem.getValue().trim();
				}

				else if(updateItem.getKey().equals("subject_dsa")) {
					creator_descipline = updateItem.getValue().trim();
				}
				else if(updateItem.getKey().equals("organ")) {
					creator_institution = updateItem.getValue().trim();
				}
				else if(updateItem.getKey().equals("contributor")) {
					contributor = updateItem.getValue().trim();
				}

				else if(updateItem.getKey().equals("abstract")) {
					description = updateItem.getValue().trim();
				}

				else if(updateItem.getKey().equals("keyword")) {
					subject = updateItem.getValue().trim();
				}
				else if(updateItem.getKey().equals("clc_no")) {
					subject_clc = updateItem.getValue().trim();
				}

				else if (updateItem.getKey().equals("pub_year")) {
					date = updateItem.getValue().trim();
				}
		
				else if (updateItem.getKey().equals("fund")) {
					description_fund = updateItem.getValue().trim();
				}

				else if  (updateItem.getKey().equals("provider_url")) {
					provider_url = updateItem.getValue().trim();
					dbcode =provider_url.split("&")[0].replace("http://kns.cnki.net/kcms/detail/detail.aspx?dbcode=", "");
				}
				}
				
				if (creator_degree.equals("博士")) {
					dbcode = "CDFD";
				} else if (creator_degree.equals("硕士")) {
					dbcode = "CMFD";
				}

					
			if(title.length() <1) {
				context.getCounter("map", "no title").increment(1);
				return;
			}

			// 去除关键字中多余的;号

			XXXXObject xObjOut = new XXXXObject();
			{
				xObj.data.put("dbcode", dbcode);
				xObj.data.put("rawid", rawid);
				xObj.data.put("title", title);
				xObj.data.put("creator", creator);
				xObj.data.put("creator_degree", creator_degree);
				xObj.data.put("creator_descipline", creator_descipline);
				xObj.data.put("creator_institution", creator_institution);
				xObj.data.put("contributor", contributor);
				xObj.data.put("description", description);
				xObj.data.put("subject_clc", subject_clc);
				xObj.data.put("subject", subject);
				xObj.data.put("date", date);
				xObj.data.put("description_fund", description_fund);
			}
			context.getCounter("map", "count").increment(1);

			byte[] bytes = VipcloudUtil.SerializeObject(xObjOut);
			context.write(new Text(rawid), new BytesWritable(bytes));
		}
	}
	public static class ProcessReducer extends
			Reducer<Text, BytesWritable, Text, BytesWritable> {

		public void reduce(Text key, Iterable<BytesWritable> values, Context context)
				throws IOException, InterruptedException {

			BytesWritable bOut = new BytesWritable();
			for (BytesWritable item : values) {
				if (item.getLength() > bOut.getLength()) { // 选最大的一个
					bOut.set(item.getBytes(), 0, item.getLength());
				}
			}

			context.getCounter("reducer", "count").increment(1);
			bOut.setCapacity(bOut.getLength()); // 将buffer设为实际长度
			context.write(key, bOut);
		}
	}
}
