package simple.jobstream.mapreduce.site.cnki_hy;
//提取特定数据
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.BXXXXObject;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;


public class B2XObject extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 40;

	private static int reduceNum = 40;
	
	public static  String inputHdfsPath = "";
	//public static final String inputHdfsPath = "/VipProcessData/BasicObject/TitleObject";
	//public static final String inputHdfsPath = "/VipProcessData/BasicInfo/TitleInfo/TitleInfo";
	public static  String outputHdfsPath = "";
	

	
	public void pre(Job job) {
		String jobName = "B2XObject";
		job.setJobName(jobName);
		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
	}

	public void post(Job job) {

	}

	public String getHdfsInput() {
		return inputHdfsPath;
	}

	public String getHdfsOutput() {
		return outputHdfsPath;
	}

	public void SetMRInfo(Job job) {
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(ProcessReducer.class);

		SequenceFileOutputFormat.setCompressOutput(job, false);

		if (testRun) {
			job.setNumReduceTasks(testReduceNum);
		} else {
			job.setNumReduceTasks(reduceNum);
		}
	}

	// ======================================处理逻辑=======================================
	public static class ProcessMapper extends
			Mapper<Text, BytesWritable, Text, BytesWritable> {
		
		public void setup(Context context) throws IOException,
				InterruptedException {
	        
		}

		public void cleanup(Context context) throws IOException,
				InterruptedException {
			
		}

		static int cnt = 0;	
		public void map(Text key, BytesWritable values, Context context)
				throws IOException, InterruptedException {
			BXXXXObject bxObj = new BXXXXObject();
			VipcloudUtil.DeserializeObject(values.getBytes(), bxObj);
			
			String rawid = "";//id
			String title_c = "";//主题
			String years="";//会议年份
			String hymeetingrecordname="";//母体文献
			String Showwriter = "";//作者
			String Showorgan = "";//机构
			String media_c = "";//会议名称
			String hymeetingplace = "";//会议地点
			String keyword_c = "";//关键词
			String remark_c = "";//摘要
			String hypressorganization = "";//主办单位
			String description_fund = "";//基金
			String flh = ""; //分类号
			String hymeetingdate = "";
			
			BXXXXObject xObj = new BXXXXObject();
			VipcloudUtil.DeserializeObject(values.getBytes(), xObj);
			
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("文件名")) {
					rawid = updateItem.getValue().trim();
				}
				else if  (updateItem.getKey().equals("主题")) {
					title_c = updateItem.getValue().trim();
					title_c = title_c.replace("'", "''");
				}
				else if (updateItem.getKey().equals("年")) {
					years = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("会议录名称")) {
					hymeetingrecordname = updateItem.getValue().trim();
					hymeetingrecordname = hymeetingrecordname.replace("'", "''").replaceAll(";$", "");
				}

				else if (updateItem.getKey().equals("中英文作者")) {
					Showwriter = updateItem.getValue().trim();
					Showwriter = Showwriter.replace("'", "''").replace("\n", "").replaceAll("\\s*；\\s*", ";").replaceAll(";$", "");
					
				}

				else if (updateItem.getKey().equals("会议名称")) {
					media_c = updateItem.getValue().trim();
					media_c = media_c.replace("'", "''").replaceAll(";$", "");
				}
				else if (updateItem.getKey().equals("会议地点")) {
					hymeetingplace = updateItem.getValue().trim();
					hymeetingplace = hymeetingplace.replace("'", "''").replaceAll(";$", "");
				}
				else if (updateItem.getKey().equals("论文关键词")) {
					keyword_c = updateItem.getValue().trim();
					keyword_c = keyword_c.replace("；", ";").replace(";;", ";").replaceAll(";$", "").replace("'", "''");
				}
				else if (updateItem.getKey().equals("中英文摘要")) {
					remark_c = updateItem.getValue().trim();
					remark_c = remark_c.replace("'", "''").replace("'", "''").replace("\n", "").replace("\t", "").replace("\r", "");
				}

				else if (updateItem.getKey().equals("主办单位")) {
					hypressorganization = updateItem.getValue().trim();
					hypressorganization = hypressorganization.replace("'", "''").replace("'", "''").replaceAll(";$", "").replace("\n", "").replace("\t", "").replace("\r", "");
				}
				else if (updateItem.getKey().equals("作者机构")) {
					Showorgan = updateItem.getValue().trim();
					Showorgan = Showorgan.replace("；", ";");
					Showorgan = Showorgan.replace("'", "''").replaceAll(";$", "").replace("\n", "").replace("\t", "").replace("\r", "");
				}
				else if (updateItem.getKey().equals("分类号")) {
					flh = updateItem.getValue().trim();
					flh = flh.replace(";", " ").replace("'", " ");
				}
				else if (updateItem.getKey().equals("发表时间")) {
					hymeetingdate = updateItem.getValue().trim();
					hymeetingdate = hymeetingdate.replace("-", "");
					hymeetingdate = hymeetingdate+"0000";
					hymeetingdate = hymeetingdate.substring(0,8);
					years = hymeetingdate.substring(0,4);
				}
				
				else if (updateItem.getKey().equals("基金")) {
					description_fund = updateItem.getValue().trim();
					description_fund = description_fund.replace("【基金】", "");
					description_fund = description_fund.replace(";;", ";").replace("'", "''").replaceAll(";$", "").replace("\n", "").replace("\t", "").replace("\r", "");
				}
				
			}
			
			XXXXObject xxObj = new XXXXObject();
			xxObj.data.put("rawid", rawid);
			xxObj.data.put("title_c",title_c);
			xxObj.data.put("years", years);
			xxObj.data.put("hymeetingrecordname",hymeetingrecordname);
			xxObj.data.put("Showwriter", Showwriter);
			xxObj.data.put("Showorgan", Showorgan);
			xxObj.data.put("media_c", media_c);
			xxObj.data.put("hymeetingplace",hymeetingplace);
			xxObj.data.put("keyword_c", keyword_c);
			xxObj.data.put("hypressorganization", hypressorganization);
			xxObj.data.put("remark_c", remark_c);
			xxObj.data.put("hymeetingdate", hymeetingdate);
			xxObj.data.put("flh", flh);
			xxObj.data.put("description_fund",description_fund);
			
			byte[] bytes = com.process.frame.util.VipcloudUtil.SerializeObject(xxObj);
			context.write(new Text(rawid), new BytesWritable(bytes));
		}
		
	}	

	public static class ProcessReducer extends
		Reducer<Text, BytesWritable, Text, BytesWritable> {
		public void reduce(Text key, Iterable<BytesWritable> values, 
		                   Context context
		                   ) throws IOException, InterruptedException {
			
			
			BytesWritable bOut = new BytesWritable();	//用于最后输出
			for (BytesWritable item : values) {
				if (item.getLength() > bOut.getLength()) {	//ѡ����һ��
					bOut.set(item.getBytes(), 0, item.getLength());
				}
			}
			
			context.getCounter("reduce", "count").increment(1);			
			
			bOut.setCapacity(bOut.getLength()); 	//��buffer��Ϊʵ�ʳ���
		
			context.write(key, bOut);
		}
	}
}
