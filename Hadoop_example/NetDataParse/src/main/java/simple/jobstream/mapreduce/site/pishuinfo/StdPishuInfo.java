package simple.jobstream.mapreduce.site.pishuinfo;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.vip.SqliteReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;

//输入应该为去重后的html
public class StdPishuInfo extends InHdfsOutHdfsJobInfo {
	
	private static int reduceNum = 1;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";
		
	public void pre(Job job) {
		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
		reduceNum = Integer.parseInt(job.getConfiguration().get("reduceNum"));
		job.setJobName(job.getConfiguration().get("jobName"));
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
		
		//job.setInputFormatClass(SimpleTextInputFormat.class);
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
	public static class ProcessMapper extends Mapper<Text, BytesWritable, Text, NullWritable> {
		
		
		
		public void setup(Context context) throws IOException,
				InterruptedException {
		}

		public void cleanup(Context context) throws IOException,
				InterruptedException {
			
		}		
		
		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			
				
			String title = "";//标题
//			String title_alternative="";
			String creator = "";//作者
			String creator_bio = "";//作者简介
			String publisher = "";//发行商
			String identifier_pisbn = "";//isbn号
			String date="";//出版日期
			String creator_institution="";//机构
			String description = "";//内容提要
			String description_en = "";
			String provider_subject = "";
			String rawid = "";//
			String subject = "";
			String subject_en = "";
			String title_series = "";
			String subject_clc = "";
			String page = "";
			
			
            String source = "";
            String identifier_pissn = "";
            String identifier_eissn = "";

		    String lngID = "";
		    String batch="";
		    String gch="";
		    String provider="pishuinfo";
		    String provider_url="";
		    String provider_id="";
		    String country="CN";
		    String cover="";
		    
		    String title_edition=""; //版本说明
		    String date_created="";
			String language = "ZH";
			String type = "14";
			String medium = "2";
			
			String volume = "";
			String issue = "";
			String identifier_cnno = "";
			String beginpage = "";
			String endpage = "";
			String jumppage = "";
			String description_fund = "";
			String cited_cnt = "";
			String source_en = "";
			String pagecount = "";
			

			
			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("provider_url")) {
					provider_url = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("author")) {
					creator = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("pub_date")) {
					date_created = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("author_intro")) {
					creator_bio = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("journal_name")) {
					source = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("publisher")) {
					publisher = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("keyword")) {
					subject = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("keyword_alt")) {
					subject_en = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("abstract")) {
					description = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("abstract_alt")) {
					description_en = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("title_series")) {
					title_series = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("page_cnt")) {
					pagecount = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("lngid")) {
					lngID = updateItem.getValue().trim();
				}
			}
			rawid = key.toString();			

			
			Date dt = new Date();
			DateFormat df = new SimpleDateFormat("yyyyMMdd");
			String nowDate = df.format(dt);
			batch = nowDate+"00";
			
			provider_url = provider + "@" + provider_url;
			provider_id = provider + "@" + rawid;

//			lngID = "CHAOXING_QK_" + rawid;
			if (!date_created.equals("")) {
				date = date_created.substring(0, 4).trim();
			}
			else {
				date = "1900";
				date_created = "19000000";
			}
			
			

			//date_created = date + "0000";
			
			//转义sql字符
//			title = StringEscapeUtils.escapeSql(title.replaceAll("《","").replaceAll("》","")).trim();//标题
//			title_alternative = StringEscapeUtils.escapeSql(title_alternative.replaceAll("《","").replaceAll("》","")).trim();//标题
//			creator = StringEscapeUtils.escapeSql(creator.replaceAll("；",";").replaceAll("，",";").replaceAll("; ", ";").replaceAll(", ",";").replaceAll(",","")).trim();//作者
			//identifier_pisbn = StringEscapeUtils.escapeSql(identifier_pisbn).trim();//isbn号

//			subject=StringEscapeUtils.escapeSql(subject);//关键字
//			description = StringEscapeUtils.escapeSql(description).trim();//内容提要
//			provider_subject = StringEscapeUtils.escapeSql(provider_subject).trim();
			//publishers = StringEscapeUtils.escapeSql(publishers).trim();
			

			
			
			title = title.replace('\0', ' ').replace("'", "''").trim();
			title_series = title_series.replace('\0', ' ').replace("'", "''").trim();
			creator = creator.replace('\0', ' ').replace("'", "''").trim();
			creator_bio = creator_bio.replace('\0', ' ').replace("'", "''").trim();
			description = description.replace('\0', ' ').replace("'", "''").trim();	
			description_en = description_en.replace('\0', ' ').replace("'", "''").trim();
			creator_institution = creator_institution.replace('\0', ' ').replace("'", "''").trim();
			subject = subject.replace('\0', ' ').replace("'", "''").trim();
			subject_en = subject_en.replace('\0', ' ').replace("'", "''").trim();
			description_fund = description_fund.replace('\0', ' ').replace("'", "''").trim();
			provider_subject = provider_subject.replace('\0', ' ').replace("'", "''").trim();
			publisher = publisher.replace('\0', ' ').replace("'", "''").trim();
			source_en = source_en.replace('\0', ' ').replace("'", "''").trim();
			source = source.replace('\0', ' ').replace("'", "''").trim();
			subject_clc = subject_clc.replace('\0', ' ').replace("'", "''").trim();
			
			
	   
			String sql = "insert into modify_title_info_zt(lngid,rawid,title,title_series,creator,creator_bio,pagecount,source,subject,subject_en,description,description_en,date,date_created,publisher,language,country,provider,provider_url,provider_id,type,medium,batch)";
			sql += " VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');";
			sql = String.format(sql, lngID,rawid,title,title_series,creator,creator_bio,pagecount,source,subject,subject_en,description,description_en,date,date_created,publisher,language,country,provider,provider_url,provider_id,type,medium,batch);					
			
			context.getCounter("map", "count").increment(1);
			//String lineOutput = AccessionNumber + "\t" + Authors + "\t" + AuthorAffiliation + "\t" + CorrAuthorAffiliation;
			context.write(new Text(sql), NullWritable.get());
			
		}
	}
}