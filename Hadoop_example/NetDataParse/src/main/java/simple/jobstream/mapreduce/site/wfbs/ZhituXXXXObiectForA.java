package simple.jobstream.mapreduce.site.wfbs;

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

import simple.jobstream.mapreduce.common.vip.LogMR;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;
import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.util.StringHelper;

// 将以往数据转为A层格式
public class ZhituXXXXObiectForA extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 30;

	public static String inputHdfsPath = "/RawData/wanfang/bs/latest";
	public static String outputHdfsPath = "/RawData/wanfang/bs/latestA";

	public void pre(Job job) {
		String jobName = "wanfang_bs." + this.getClass().getSimpleName();
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
		// job.setReducerClass(ProcessReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);

		// job.setInputFormatClass(SimpleTextInputFormat.class);
		// job.setOutputFormatClass(TextOutputFormat.class);

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
		private static String rawid = "";
		private static String title_c = "";
		private static String Showwriter = "";
		private static String bsspeciality = "";
		private static String bsdegree = "";
		private static String Showorgan = "";
		private static String bstutorsname = "";
		private static String years = "";
		private static String classFid = "";
		private static String keyword_c = "";
		private static String remark_c = "";
		private static String marksNumber = "";
		private static String doi = "";
		private static String date_created="";
		private static String source_type="4";
		private static String language="ZH";
		private static String country="CN";
		private static String author_1st="";
		private static String clc_no_1st="";
		private static String logHDFSFile = "/user/qianjun/log/log_map/" + DateTimeHelper.getNowDate() + ".txt";
		// 重新生成batch
		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
			{// 老字段
				rawid = "";
				title_c = "";
				Showwriter = "";
				bsspeciality = "";
				bsdegree = "";
				Showorgan = "";
				bstutorsname = "";
				years = "";
				classFid = "";
				keyword_c = "";
				remark_c = "";
				marksNumber = "";
				doi = "";
				date_created="";
				source_type="4";
				language="ZH";
				country="CN";
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
				else if (updateItem.getKey().equals("title_c")) {
					title_c = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Showwriter")) {
					Showwriter = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("bsspeciality")) {
					bsspeciality = updateItem.getValue().trim();
				}

				else if (updateItem.getKey().equals("bsdegree")) {
					bsdegree = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Showorgan")) {
					Showorgan = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("bstutorsname")) {
					bstutorsname = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("years")) {
					years = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("class")) {
					classFid = updateItem.getValue().trim();
				}

				else if (updateItem.getKey().equals("remark_c")) {
					remark_c = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("keyword_c")) {
					keyword_c = updateItem.getValue().trim();
				}

				else if (updateItem.getKey().equals("marksNumber")) {
					marksNumber = updateItem.getValue().trim();
				}

				else if (updateItem.getKey().equals("doi")) {
					doi = updateItem.getValue().trim();
				}

			}
			context.getCounter("map", "count all").increment(1);
			if(years.length() ==4) {
				date_created = years+ "0000";
			}else {
				LogMR.log2HDFS4Mapper(context, logHDFSFile, "error:bad year: " + years);
				years="1900";
				date_created = years + "0000";}
			// 处理第一作者
			if(Showwriter.contains(";")) {
				author_1st = Showwriter.split(";")[0];			
			}
			else {
				if(Showwriter.length() >1) {
					author_1st=Showwriter;
				}
				
			}
			
			classFid = classFid.replace("，", ";").replace(" ", ";").trim();
			if(classFid.endsWith(";")) {
				classFid = classFid.replaceAll(";$", "");
				context.getCounter("map", "end with ;号").increment(1);
			}
			// 处理第一中图分类号
			if(classFid.contains(";")) {
				clc_no_1st =classFid.split(";")[0];			
			}
			else {
				if(classFid.length() >0) {
					clc_no_1st =classFid;
				}
				
			}
			
			// 去除关键字中多余的;号
			keyword_c =StringHelper.cleanSemicolon(keyword_c);
			
//			if(rawid.length() <1) {
//				context.getCounter("map", "not rawid").increment(1);
//				return;
//			}
//			if(title_c.length() <1) {
//				context.getCounter("map", "no title").increment(1);
//				return;
//			}else {
//				context.getCounter("map", "has title").increment(1);
//			}
			XXXXObject xObjOut = new XXXXObject();
			{
				xObjOut.data.put("lngid", VipIdEncode.getLngid("00005", rawid, false));
				xObjOut.data.put("rawid", rawid);
				xObjOut.data.put("sub_db_id", "00005");
				xObjOut.data.put("product", "WANFANG");
				xObjOut.data.put("sub_db", "CDDB");
				xObjOut.data.put("provider", "WANFANG");
				xObjOut.data.put("down_date", "20190101");
				xObjOut.data.put("batch", "20190529_010101");
				xObjOut.data.put("doi", "");
				xObjOut.data.put("source_type", "4");
				xObjOut.data.put("provider_url", "http://www.wanfangdata.com.cn/details/detail.do?_type=degree&id="+rawid);
				xObjOut.data.put("title", title_c);
				xObjOut.data.put("title_alt", "");
				xObjOut.data.put("title_sub", "");
				xObjOut.data.put("keyword", keyword_c);
				xObjOut.data.put("keyword_alt", "");
				xObjOut.data.put("keyword_machine", "");
				xObjOut.data.put("clc_no_1st", clc_no_1st);
				xObjOut.data.put("clc_no", classFid);
				xObjOut.data.put("clc_machine", "");
				xObjOut.data.put("subject_word", "");
				xObjOut.data.put("subject_edu", "");
				xObjOut.data.put("subject", "");
				xObjOut.data.put("abstract", remark_c);
				xObjOut.data.put("abstract_alt", "");
				xObjOut.data.put("recv_date", "");
				xObjOut.data.put("pub_date", date_created);
				xObjOut.data.put("pub_date_alt", "");
				xObjOut.data.put("defense_date", "");
				xObjOut.data.put("impl_date", "");
				xObjOut.data.put("secrecy", "");
				xObjOut.data.put("page_cnt", "");
				xObjOut.data.put("pdf_size", "");
				xObjOut.data.put("fulltext_txt", "");
				xObjOut.data.put("fulltext_addr", "");
				xObjOut.data.put("fulltext_type", "");
				xObjOut.data.put("fund", "");
				xObjOut.data.put("author_id", "");
				xObjOut.data.put("author_1st", author_1st);
				xObjOut.data.put("author", Showwriter);
				xObjOut.data.put("author_alt", "");
				xObjOut.data.put("subject_dsa", bsspeciality);
				xObjOut.data.put("degree_apply", "");
				xObjOut.data.put("degree", bsdegree);
				xObjOut.data.put("research_field", "");
				xObjOut.data.put("contributor", bstutorsname);
				xObjOut.data.put("contributor_id", "");
				xObjOut.data.put("contributor_alt", "");
				xObjOut.data.put("author_intro", "");
				xObjOut.data.put("organ_id", "");
				xObjOut.data.put("organ", Showorgan);
				xObjOut.data.put("organ_alt", "");
				xObjOut.data.put("school_code", "");
				xObjOut.data.put("depart", "");
				xObjOut.data.put("organ_area", "");
				xObjOut.data.put("pub_year", years);
				xObjOut.data.put("publisher", "");
				xObjOut.data.put("is_oa", "");
				xObjOut.data.put("country", country);
				xObjOut.data.put("language", language);
				xObjOut.data.put("ref_cnt", "");
				xObjOut.data.put("ref_id", "");
				xObjOut.data.put("down_cnt", "");
			}
			context.getCounter("map", "count").increment(1);

			byte[] bytes = VipcloudUtil.SerializeObject(xObjOut);
			context.write(new Text(rawid), new BytesWritable(bytes));
		}
	}

}
