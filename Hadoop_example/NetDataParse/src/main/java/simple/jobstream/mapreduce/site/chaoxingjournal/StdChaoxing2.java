package simple.jobstream.mapreduce.site.chaoxingjournal;

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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.vip.SqliteReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;

//输入应该为去重后的html
public class StdChaoxing2 extends InHdfsOutHdfsJobInfo {
	
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
		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.7f);
		
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
		
		private static Map<String, String> gchmap = new HashMap<String, String>();
		
		private static void initArrayList(Context context) throws IOException {
			FileSystem fs = FileSystem.get(context.getConfiguration());

			FSDataInputStream fin = fs.open(new Path("/RawData/chaoxing/chaoxingjournal/mags/mags.txt"));

			BufferedReader reader = null;
			try {
				reader = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
				String temp;

				while ((temp = reader.readLine()) != null) {
					String[] vec = temp.split("\t");
					if (vec.length != 2) {
						continue;
					}

					gchmap.put(vec[0].toLowerCase(), vec[1].toUpperCase());						
					
				}

			} catch (IOException e) {
				// TODO Auto-generated catch block
			} finally {
				if (reader != null) {
					reader.close();
				}
			}
			System.out.println("*******utsetsize:" + gchmap.size());

		}
		
		
		
		public void setup(Context context) throws IOException,
				InterruptedException {
			initArrayList(context);
		}

		public void cleanup(Context context) throws IOException,
				InterruptedException {
			
		}		
		
		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			
			String url = "";
				
			String title = "";//标题
//			String title_alternative="";
			String creator = "";//作者
			String publisher = "";//发行商
			String identifier_pisbn = "";//isbn号
			String date="";//出版日期
			String creator_institution="";//机构
			String description = "";//内容提要
			String provider_subject = "";
			String rawid = "";//
			String subject = "";
			String title_series = "";
			String subject_clc = "";
			String page = "";
			
			
            String source = "";
            String identifier_pissn = "";
            String identifier_eissn = "";

		    String lngID = "";
		    String batch="";
		    String gch="";
		    String provider="chaoxingjournal";
		    String provider_url="";
		    String provider_id="";
		    String country="CN";
		    String cover="";
		    
		    String title_edition=""; //版本说明
		    String date_created="";
			String language = "ZH";
			String type = "3";
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
				else if (updateItem.getKey().equals("issn")) {
					identifier_pissn = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("vol")) {
					volume = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("num")) {
					issue = updateItem.getValue().trim();
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
				else if (updateItem.getKey().equals("cnno")) {
					identifier_cnno = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("subject")) {
					provider_subject = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("organ")) {
					creator_institution = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("begin_page")) {
					beginpage = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("end_page")) {
					endpage = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("jump_page")) {
					jumppage = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("clc_no")) {
					subject_clc = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("keyword")) {
					subject = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("fund")) {
					description_fund = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("abstract")) {
					description = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("cited_cnt")) {
					cited_cnt = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("journal_id")) {
					gch = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("journal_name_alt")) {
					source_en = updateItem.getValue().trim();
				}
			}
			rawid = key.toString();			

			
			Date dt = new Date();
			DateFormat df = new SimpleDateFormat("yyyyMMdd");
			String nowDate = df.format(dt);
			batch = nowDate+"00";
			
//			url = "http://qikan.chaoxing.com" + url;
			provider_url = provider + "@" + provider_url;
			provider_id = provider + "@" + rawid;
			if (gchmap.containsKey(gch.toLowerCase())) {
				gch = gchmap.get(gch.toLowerCase());
			}
			else {
				gch = "";
			}
			lngID = VipIdEncode.getLngid("00006",rawid,false);
//			lngID = "CHAOXING_QK_" + rawid;
			if (!date_created.equals("")) {
				date = date_created.substring(0, 4).trim();
			}
			else {
				date = "1900";
				date_created = "19000000";
			}
			
						

			
			
			title = title.replace('\0', ' ').replace("'", "''").trim();
			creator = creator.replace('\0', ' ').replace("'", "''").trim();
			description = description.replace('\0', ' ').replace("'", "''").trim();			
			creator_institution = creator_institution.replace('\0', ' ').replace("'", "''").trim();
			subject = subject.replace('\0', ' ').replace("'", "''").trim();
			description_fund = description_fund.replace('\0', ' ').replace("'", "''").trim();
			provider_subject = provider_subject.replace('\0', ' ').replace("'", "''").trim();
			publisher = publisher.replace('\0', ' ').replace("'", "''").trim();
			source_en = source_en.replace('\0', ' ').replace("'", "''").trim();
			source = source.replace('\0', ' ').replace("'", "''").trim();
			subject_clc = subject_clc.replace('\0', ' ').replace("'", "''").trim();
			
			
	   
			String sql = "insert into modify_title_info_zt(lngid,rawid,title,creator,gch,source_en,source,beginpage,endpage,jumppage,identifier_pissn,subject_clc,description_fund,cited_cnt,subject,description,date,date_created,volume,issue,publisher,identifier_cnno,creator_institution,provider_subject,language,country,provider,provider_url,provider_id,type,medium,batch)";
			sql += " VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');";
			sql = String.format(sql, lngID,rawid,title,creator,gch,source_en,source,beginpage,endpage,jumppage,identifier_pissn,subject_clc,description_fund,cited_cnt,subject,description,date,date_created,volume,issue,publisher,identifier_cnno,creator_institution,provider_subject,language,country,provider,provider_url,provider_id,type,medium,batch);					
			
			context.getCounter("map", "count").increment(1);
			//String lineOutput = AccessionNumber + "\t" + Authors + "\t" + AuthorAffiliation + "\t" + CorrAuthorAffiliation;
			context.write(new Text(sql), NullWritable.get());
			
		}
	}
}