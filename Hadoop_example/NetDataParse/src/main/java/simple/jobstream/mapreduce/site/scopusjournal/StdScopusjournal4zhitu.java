package simple.jobstream.mapreduce.site.scopusjournal;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
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

import simple.jobstream.mapreduce.common.vip.AuthorOrgan;
import simple.jobstream.mapreduce.common.vip.SqliteReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;
 


//输入应该为去重后的html
public class StdScopusjournal4zhitu extends InHdfsOutHdfsJobInfo {

	private static int reduceNum = 1;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";

	public void pre(Job job) {
		String jobName = "plosjournal." + this.getClass().getSimpleName();
//		String jobName = job.getConfiguration().get("jobName");
		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
		reduceNum = Integer.parseInt(job.getConfiguration().get("reduceNum"));
		job.setJobName(jobName);
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

		// job.setInputFormatClass(SimpleTextInputFormat.class);
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

		public void setup(Context context) throws IOException, InterruptedException {

		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {

		}

		// 记录日志到HDFS
		public boolean log2HDFSForMapper(Context context, String text) {
			Date dt = new Date();// 如果不需要格式,可直接用dt,dt就是当前系统时间
			DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");// 设置显示格式
			String nowTime = df.format(dt);// 用DateFormat的format()方法在dt中获取并以yyyy/MM/dd HH:mm:ss格式显示

			df = new SimpleDateFormat("yyyyMMdd");// 设置显示格式
			String nowDate = df.format(dt);// 用DateFormat的format()方法在dt中获取并以yyyy/MM/dd HH:mm:ss格式显示

			text = nowTime + "\n" + text + "\n\n";

			boolean bException = false;
			BufferedWriter out = null;
			try {
				// 获取HDFS文件系统
				FileSystem fs = FileSystem.get(context.getConfiguration());

				FSDataOutputStream fout = null;
				String pathfile = "/lqx/log/log_map/" + nowDate + ".txt";
				if (fs.exists(new Path(pathfile))) {
					fout = fs.append(new Path(pathfile));
				} else {
					fout = fs.create(new Path(pathfile));
				}

				out = new BufferedWriter(new OutputStreamWriter(fout, "UTF-8"));
				out.write(text);
				out.close();

			} catch (Exception ex) {
				bException = true;
			}

			if (bException) {
				return false;
			} else {
				return true;
			}
		}

		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
			
			String lngid = "";
			String rawid = "";   
			String batch = "";
			String title = ""; 
			//String title_series = ""; 
			String identifier_pisbn = "";
			String identifier_pissn = "";
			String identifier_doi = "";
			String creator = "";//作者信息
			String creator_institution = "";//机构信息
			//String creator_bio = "";//作者简介
			String source = "";//journal_name
 			//String source_id = "";//journal_raw_id
			String publisher = "";
			String date = "";//year
			String volume = "";
			String issue = "";
			String description = "";
			String description_fund = "";
			String subject = "";//keyword
//			String page = "";
			String beginpage = "";
			String endpage = ""; 
			//String jumppage = "";
			String pagecount = "";
			String date_created = ""; //pubdate
//			String if_pdf_fulltext = "";
			String ref_cnt = ""; 
			String cited_cnt = ""; 
//			String down_cnt = ""; 
			 
			String rawtype = ""; 
			String is_oa = "";
			String provider_url = ""; 
			String country = ""; 
			String language = "";
			 
			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("lngid")) {
					lngid = updateItem.getValue().trim();
				}if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("batch")) {
					batch = updateItem.getValue().trim().split("_")[0]+"00";
				} else if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("isbn")) {
					identifier_pisbn = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("issn")) {
					identifier_pissn = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("doi")) {
					identifier_doi = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("author")) {
					creator = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("organ")) {
					creator_institution = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("journal_name")) {
					source = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("publisher")) {
					publisher = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pub_year")) {
					date = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("vol")) {
					volume = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("num")) {
					issue = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("keyword")) {
					subject = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("abstract")) {
					description = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("fund")) {
					description_fund = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("begin_page")) {
					beginpage = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("end_page")) {
					endpage = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("page_cnt")) {
					pagecount = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pub_date")) {
					date_created = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("cited_cnt")) {
					cited_cnt = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("ref_cnt")) {
					ref_cnt = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("raw_type")) {
					rawtype = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("is_oa")) {
					is_oa = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("provider_url")) {
					provider_url = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("country")) {
					country = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("language")) {
					language = updateItem.getValue().trim();
				}  
			}
			
			if (title.length() < 2) {
				return;
			}   
			
			
			String medium = "1"; //实体或数字  
			String provider = "scopusjournal";
			String provider_id = provider + "@" + rawid;
			provider_url = provider + "@" + provider_url;
			//String gch = provider + "@" + source_id;
			String type = "3";
			 
			if (title.length() < 2) {
				return;
			}
		 
			rawid = rawid.replace('\0', ' ').replace("'", "''").trim();  
			title = title.replace('\0', ' ').replace("'", "''").trim();
			identifier_doi = identifier_doi.replace('\0', ' ').replace("'", "''").trim(); 
			creator = creator.replace('\0', ' ').replace("'", "''").trim();
			creator_institution = creator_institution.replace('\0', ' ').replace("'", "''").trim(); 
			source = source.replace('\0', ' ').replace("'", "''").trim(); 
			publisher = publisher.replace('\0', ' ').replace("'", "''").trim(); 
			description = description.replace('\0', ' ').replace("'", "''").trim(); 
			description_fund = description_fund.replace('\0', ' ').replace("'", "''").trim();
			subject = subject.replace('\0', ' ').replace("'", "''").trim();  
			
			String sql = "insert into modify_title_info_zt("
					+ "lngid,rawid,batch,title,identifier_pisbn,identifier_pissn,"
					+ "identifier_doi,creator,creator_institution,source,publisher,"
					+ "date,volume,issue,description,description_fund," 
					+ "subject,beginpage,endpage,pagecount,date_created,"
					+ "ref_cnt,cited_cnt,rawtype,is_oa,medium,"
					+ "provider,provider_id,provider_url,type,country,language)";
			sql += " VALUES ('%s','%s','%s','%s','%s','%s',"
					+ "'%s','%s','%s','%s','%s',"
					+ "'%s','%s','%s','%s','%s',"
					+ "'%s','%s','%s','%s','%s',"
					+ "'%s','%s','%s','%s','%s',"
					+ "'%s','%s','%s','%s','%s','%s');";
			sql = String.format(sql,lngid,rawid,batch,title,identifier_pisbn,identifier_pissn,
					identifier_doi,creator,creator_institution,source,publisher,
					date,volume,issue,description,description_fund,
					subject,beginpage,endpage,pagecount,date_created,
					ref_cnt,cited_cnt,rawtype,is_oa,medium,
					provider,provider_id,provider_url,type,country,language);

			context.getCounter("map", "count").increment(1);
			context.write(new Text(sql), NullWritable.get());
		}
	}

}