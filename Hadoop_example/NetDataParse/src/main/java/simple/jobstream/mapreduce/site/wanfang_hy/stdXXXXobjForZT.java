package simple.jobstream.mapreduce.site.wanfang_hy;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;




import org.json.JSONArray;
import org.json.JSONObject;



import com.almworks.sqlite4java.SQLiteConnection;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.JobConfUtil;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.vip.SqliteReducer;
import simple.jobstream.mapreduce.site.pubmed.StdPubmedZhiTu.ProcessMapper;

//输入应该为去重后的html
//不需修改
public class stdXXXXobjForZT extends InHdfsOutHdfsJobInfo {

	private static boolean testRun = true;
	private static int testReduceNum = 1;
	private static int reduceNum = 1;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";

	public void pre(Job job) {
		String jobName = "hy_zt";
		if (testRun) {
			jobName = "test_" + jobName;
		}

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
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));
		job.setOutputValueClass(BytesWritable.class);
		JobConfUtil.setTaskPerReduceMemory(job, 6144);

		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(SqliteReducer.class);

		TextOutputFormat.setCompressOutput(job, false);

		job.setNumReduceTasks(reduceNum);
	}

	// ======================================处理逻辑=======================================
	public static class ProcessMapper extends
			Mapper<Text, BytesWritable, Text, NullWritable> {

		private static Map<String, String> monthMap = new HashMap<String, String>();
	
		public String getMapValueByKey(String mykey) {
			String value = "00";
			for (Map.Entry entry : monthMap.entrySet()) {

				String key = entry.getKey().toString();
				if (mykey.toLowerCase().startsWith(key)) {
					value = entry.getValue().toString();
					break;

				}

			}
			return value;

		}

		public void setup(Context context) throws IOException,
				InterruptedException {
		}

	
		public static String  wanID2vipID(String wanID){
			String vipID = "";
			for(char a : wanID.toCharArray()){
				if('0' <= a && a <= '9'){
					vipID = vipID + a;
				}else{
					vipID = vipID + Integer.toString(0 + a);
				}
				
				
			}
			vipID = "W_HY_"+ Long.toString( Long.parseLong(vipID)*2 + 3);
			return vipID;
		}
		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {

			String rawid = "";//id
			String title = "";//主题
			String date="";//会议年份
			String title_series="";//母体文献
			String creator = "";//作者
			String creator_institution = "";//机构
			String source = "";//会议名称
			String source_institution = "";//会议地点
			String subject = "";//关键词
			String description = "";//摘要
			String creator_release = "";//主办单位
			String description_fund = "";//基金
			String language = "ZH";//语言
			String type = "6";
			String country = "CN";
			String lngID = "";
			String subject_clc = ""; //分类号
			String date_created = "";
			String owner = "cqu";
			String year = "";
			String month = "";
			String day = "";
			String provider_url="";
			String provider = "wanfangconference";
			
			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				}
				else if(updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
					title = title.replace("'", "''");
				}
				else if(updateItem.getKey().equals("pub_year")) {
					date = updateItem.getValue().trim();
				}
				else if(updateItem.getKey().equals("meeting_record_name")) {
					title_series = updateItem.getValue().trim();
					title_series = title_series.replace("'", "''");
				}

				else if(updateItem.getKey().equals("author")) {
					creator = updateItem.getValue().trim();
					creator = creator.replace("'", "''");
				}

				else if(updateItem.getKey().equals("meeting_name")) {
					source = updateItem.getValue().trim();
					source = source.replace("'", "''");
				}
				else if(updateItem.getKey().equals("meeting_place")) {
					source_institution = updateItem.getValue().trim();
					source_institution = source_institution.replace("'", "''");
				}
				else if(updateItem.getKey().equals("keyword")) {
					subject = updateItem.getValue().trim();
					subject = subject.replace("；", ";").replaceAll(";$", "");
				}
				else if(updateItem.getKey().equals("abstract")) {
					description = updateItem.getValue().trim();
					description = description.replace("'", "''");
				}

				else if(updateItem.getKey().equals("host_organ")) {
					creator_release = updateItem.getValue().trim();
					creator_release = creator_release.replace("'", "''");
				}
				else if(updateItem.getKey().equals("organ")) {
					creator_institution = updateItem.getValue().trim();
					creator_institution = creator_institution.replace("'", "''");
				}
				else if(updateItem.getKey().equals("clc_no")) {
					subject_clc = updateItem.getValue().trim();
					subject_clc = subject_clc.replace('\0', ' ').replace("'", "''").replace("，", ";").replace(" ", ";").trim();
				}
				else if(updateItem.getKey().equals("accept_date")) {
					//date_created = date_created.replace("-", "");
					date_created = updateItem.getValue().trim();
				}
			
				else if(updateItem.getKey().equals("lngid")) {
		
					lngID = updateItem.getValue().trim();
				}
				else if(updateItem.getKey().equals("fund")) {
					description_fund = updateItem.getValue().trim();
					description_fund = description_fund.replace("【基金】", "");
				}
				else if(updateItem.getKey().equals("provider_url")) {
					provider_url=  provider +"@"+updateItem.getValue().trim();


				}
			}
			lngID = wanID2vipID(rawid);
			date = date_created.substring(0, 4);
			if (rawid.trim().length() < 1) {
				context.getCounter("map", "null rawid").increment(1);
				return;
			}
			String medium = "2";
			String batch = (new SimpleDateFormat("yyyyMMdd"))
					.format(new Date()) + "00";

			String provider_id = provider + "@" + rawid;
			String sql = "INSERT INTO modify_title_info_zt([provider_id],[provider],[provider_url],[batch],[medium],[country],[description_fund],[subject_clc],[type],[language],[lngid],[rawid], [title], [date],[title_series], [creator], [creator_institution], [source], [source_institution], [subject], [description], [creator_release],[date_created]) ";
			sql += " VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s','%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');";
			sql = String.format(sql,provider_id,provider,provider_url,
					batch,medium,country,description_fund,subject_clc,type,
					language,lngID,rawid, title, date,title_series,creator,
					creator_institution, source,source_institution, subject,
					description,creator_release,date_created);
			context.getCounter("map", "count").increment(1);
			context.write(new Text(sql), NullWritable.get());

		}
	}

}