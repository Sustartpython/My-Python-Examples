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

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.vip.VipIdEncode;
import simple.jobstream.mapreduce.common.util.StringHelper;


// 将以往数据转为A层格式
public class ZhituXXXXObiectForA extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 100;

	public static String inputHdfsPath = "/RawData/cnki/bs/latest";
	public static String outputHdfsPath = "/RawData/cnki/bs/latestA";

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
		private static String lngid="";
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
		private static String type="";
		private static String language="";
		private static String country="";
		private static String provider="";
		private static String batch="";
		private static String provider_url="";
		private static String medium="";
		private static String date="";
		private static String provider_id="";
		private static String date_created="";
		private static String description_fund="";
		private static String dbcode="";
		private static String owner="";
		private static String author_1st="";
		private static String clc_no_1st="";
		
		// 重新生成batch
		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
			{// 老字段
				lngid = "";
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
				language = "ZH";
				country = "CN";
				provider = "cnkithesis";
				provider_url = "";
				provider_id = "";
				type = "4";
				medium = "2";
				batch = "";
				dbcode = "";
				owner = "cqu";
				date_created = "";
				description_fund = "";
				author_1st="";
				clc_no_1st="";
			}

			XXXXObject xObj = new XXXXObject();
			byte[] bs = new byte[value.getLength()];  
			System.arraycopy(value.getBytes(), 0, bs, 0, value.getLength());  
			VipcloudUtil.DeserializeObject(bs, xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
					if (updateItem.getKey().equals("rawid")) {
						rawid = updateItem.getValue().trim();
					}
					if (updateItem.getKey().equals("title")) {
						title = updateItem.getValue().trim();
					}
					if (updateItem.getKey().equals("creator")) {
						creator = updateItem.getValue().trim();
					}

					if (updateItem.getKey().equals("creator_degree")) {
						creator_degree = updateItem.getValue().trim();
					}

					if (updateItem.getKey().equals("creator_descipline")) {
						creator_descipline = updateItem.getValue().trim();
					}
					if (updateItem.getKey().equals("creator_institution")) {
						creator_institution = updateItem.getValue().trim();
					}
					if (updateItem.getKey().equals("contributor")) {
						contributor = updateItem.getValue().trim();
					}

					if (updateItem.getKey().equals("description")) {
						description = updateItem.getValue().trim();
					}

					if (updateItem.getKey().equals("subject")) {
						subject = updateItem.getValue().trim();
					}
					if (updateItem.getKey().equals("subject_clc")) {
						subject_clc = updateItem.getValue().trim();
					}

					if (updateItem.getKey().equals("date")) {
						date = updateItem.getValue().trim();
					}
					
					if (updateItem.getKey().equals("dbcode")) {
						dbcode = updateItem.getValue().trim();
					}
					if(updateItem.getKey().equals("description_fund")){
						description_fund = updateItem.getValue().trim();
					}
					
				}
				
				if(creator_degree.equals("博士")){
					dbcode = "CDFD";
				}else if(creator_degree.equals("硕士")){
					dbcode = "CMFD";
				}
				if(creator_degree.length() <1) {
					dbcode = "CMFD";
				}
				if(description_fund.endsWith(";")){
					description_fund = description_fund.substring(0,description_fund.length()-1);
				}
				if(date.length() ==4) {
					date_created = date + "0000";
				}else {
					date="1900";
					date_created = date + "0000";
				}
					
			if(title.length() <1) {
				context.getCounter("map", "no title").increment(1);
				return;
			}
			// 处理第一作者
			if(creator.contains(";")) {
				author_1st = creator.split(";")[0];			
			}
			else {
				if(creator.length() >1) {
					author_1st=creator;
				}
				
			}
			if(subject_clc.endsWith(";")) {
				subject_clc= subject_clc.replaceAll(";$", "");
				context.getCounter("map", "end with ;号").increment(1);
			}
			// 处理第一中图分类号
			if(subject_clc.contains(";")) {
				clc_no_1st =subject_clc.split(";")[0];			
			}
			else {
				if(subject_clc.length() >0) {
					clc_no_1st =subject_clc;
				}
				
			}
			
			// 去除关键字中多余的;号
			subject =StringHelper.cleanSemicolon(subject);
			
	
			XXXXObject xObjOut = new XXXXObject();
			{
				xObjOut.data.put("lngid", VipIdEncode.getLngid("00075", rawid, false));
				xObjOut.data.put("rawid", rawid);
				xObjOut.data.put("sub_db_id", "00075");
				xObjOut.data.put("product", "CNKI");
				xObjOut.data.put("sub_db", "CDMD");
				xObjOut.data.put("provider", "CNKI");
				xObjOut.data.put("down_date", "20190428");
				xObjOut.data.put("batch", "20190507_010101");
				xObjOut.data.put("doi","");
				xObjOut.data.put("source_type", "4");
				xObjOut.data.put("provider_url",
						"http://kns.cnki.net/kcms/detail/detail.aspx?dbcode=" + dbcode + "&filename="  + rawid);
				xObjOut.data.put("title", title);
				xObjOut.data.put("title_alt","");
				xObjOut.data.put("title_sub", "");
				xObjOut.data.put("title_series", "");
				xObjOut.data.put("keyword", subject);
				xObjOut.data.put("keyword_alt", "");
				xObjOut.data.put("keyword_machine", "");
				xObjOut.data.put("clc_no_1st", clc_no_1st);
				xObjOut.data.put("clc_no", subject_clc);
				xObjOut.data.put("clc_machine", "");
				xObjOut.data.put("subject_word", "");
				xObjOut.data.put("subject_edu", "");
				xObjOut.data.put("subject", "");
				xObjOut.data.put("abstract", description);
				xObjOut.data.put("abstract_alt", "");
				xObjOut.data.put("abstract_type", "");
				xObjOut.data.put("abstract_alt_type", "");
				xObjOut.data.put("page_info", "");
				xObjOut.data.put("begin_page","");
				xObjOut.data.put("end_page","");
				xObjOut.data.put("jump_page", "");
				xObjOut.data.put("doc_code", "");
				xObjOut.data.put("doc_no", "");
				xObjOut.data.put("raw_type", "");
				xObjOut.data.put("recv_date", "");
				xObjOut.data.put("accept_date", "");
				xObjOut.data.put("revision_date", "");
				xObjOut.data.put("pub_date", "");
				xObjOut.data.put("pub_date_alt", "");
				xObjOut.data.put("pub_place", "");
				xObjOut.data.put("page_cnt", "");
				xObjOut.data.put("pdf_size", "");
				xObjOut.data.put("fulltext_txt", "");
				xObjOut.data.put("fulltext_addr", "");
				xObjOut.data.put("fulltext_type", "");
				xObjOut.data.put("column_info", "");
				xObjOut.data.put("fund", description_fund);
				xObjOut.data.put("fund_alt", "");
				xObjOut.data.put("author_id", "");
				xObjOut.data.put("author_1st", author_1st);
				xObjOut.data.put("author", creator);
				xObjOut.data.put("author_raw", "");
				xObjOut.data.put("author_alt", "");
				xObjOut.data.put("corr_author", "");
				xObjOut.data.put("corr_author_id", "");
				xObjOut.data.put("email", "");
				xObjOut.data.put("subject_dsa", creator_descipline);
				xObjOut.data.put("research_field", "");
				xObjOut.data.put("contributor", contributor);
				xObjOut.data.put("contributor_id", "");
				xObjOut.data.put("contributor_alt", "");
				xObjOut.data.put("author_intro", "");
				xObjOut.data.put("organ_id", "");
				xObjOut.data.put("organ_1st", "");
				xObjOut.data.put("organ", creator_institution);
				xObjOut.data.put("organ_alt", "");
				xObjOut.data.put("preferred_organ", "");
				xObjOut.data.put("host_organ_id", "");
				xObjOut.data.put("organ_area", "");
				xObjOut.data.put("journal_raw_id", "");
				xObjOut.data.put("journal_name","");
				xObjOut.data.put("journal_name_alt", "");
				xObjOut.data.put("pub_year",date);
				xObjOut.data.put("vol", "");
				xObjOut.data.put("num", "");
				xObjOut.data.put("is_suppl", "");
				xObjOut.data.put("issn", "");
				xObjOut.data.put("eissn", "");
				xObjOut.data.put("cnno", "");
				xObjOut.data.put("publisher", "");
				xObjOut.data.put("cover_path", "");
				xObjOut.data.put("is_oa", "");
				xObjOut.data.put("country", "CN");
				xObjOut.data.put("language", "ZH");
				xObjOut.data.put("ref_cnt","");
				xObjOut.data.put("ref_id", "");
				xObjOut.data.put("cited_id", "");
				xObjOut.data.put("cited_cnt", "");
				xObjOut.data.put("down_cnt", "");
				xObjOut.data.put("is_topcited", "");
				xObjOut.data.put("is_hotpaper", "");
				xObjOut.data.put("degree", creator_degree);
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
