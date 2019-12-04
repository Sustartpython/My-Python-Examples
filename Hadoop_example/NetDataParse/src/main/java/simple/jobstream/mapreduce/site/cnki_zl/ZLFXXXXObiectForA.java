package simple.jobstream.mapreduce.site.cnki_zl;

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
public class ZLFXXXXObiectForA extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 100;

	public static String inputHdfsPath = "/RawData/cnki/zl/latest";
	public static String outputHdfsPath = "/RawData/cnki/zl/latestA";

	public void pre(Job job) {
		String jobName = "cnki_zl." + this.getClass().getSimpleName();
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

		private static String logHDFSFile = "/user/qianjun/log/log_map/" + DateTimeHelper.getNowDate() + ".txt";
		// 老字段
		private static String rawid = "";
		private static String lngid = "";
		private static String title = "";
		private static String identifier_issn = "";// 申请号
		private static String date_created = ""; // 申请日
		private static String identifier_standard = "";// 公开号
		private static String date_impl = "";// 公开日
		private static String creator_cluster = "";// 申请人
		private static String creator_institution = "";// 申请人地址
		private static String creator = "";// 发明人
		private static String agency = "";// 代理机构
		private static String agents = "";// 代理人
		private static String province_code = "";// 国省代码
		private static String description = "";// 摘要
		private static String subject_csc = "";// 主分类号
		private static String subject_isc = "";// 专利分类号
		private static String language = "ZH";
		private static String country = "CN";
		private static String provider = "";
		private static String provider_url = "";
		private static String provider_id = "";
		private static String type = "7";
		private static String medium = "2";
		private static String batch = "";
		private static String date = "";
		private static String owner = "";
		private static String page = "";
		private static String description_core = "";
		private static String legal_status = "";
		private static String description_type = "";
		private static String sub_db_id = "00003";
		private static String author_1st = "";
		private static String clc_no_1st = "";
		private static String raw_type = "7";

		// 处理申请号验证位问题
		static String get_check_bit(String typedeal) {
			String bb;
			if (typedeal.length() == 8) {
				bb = "23456789";
			} else if (typedeal.length() == 12) {
				bb = "234567892345";
			} else {
				return null;
			}
			char[] ar = typedeal.toCharArray(); // char数组
			char[] br = bb.toCharArray(); // char数组
			int allnum = 0;
			for (int i = 0; i < ar.length; i++) {
				int mi = Integer.parseInt(String.valueOf(ar[i]));
				int mj = Integer.parseInt(String.valueOf(br[i]));
				allnum += mi * mj;
			}
			int mode = allnum % 11;
			String modes = "";
			if (mode == 10) {
				modes = "X";
			} else {
				modes = String.valueOf(mode);
			}
			return modes;
		}

		// 重新生成batch
		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
			{// 老字段
				rawid = "";
				lngid = "";
				title = "";
				identifier_issn = "";// 申请号
				date_created = ""; // 申请日
				identifier_standard = "";// 公开号
				date_impl = "";// 公开日
				creator_cluster = "";// 申请人
				creator_institution = "";// 申请人地址
				creator = "";// 发明人
				agency = "";// 代理机构
				agents = "";// 代理人
				province_code = "";// 国省代码
				description = "";// 摘要
				subject_csc = "";// 主分类号
				subject_isc = "";// 专利分类号
				language = "ZH";
				country = "CN";
				provider = "";
				provider_url = "";
				provider_id = "";
				type = "7";
				medium = "2";
				batch = "";
				date = "";
				owner = "";
				page = "";
				description_core = "";
				legal_status = "";
				description_type = "";
				sub_db_id = "00003";
				raw_type = "7";
			}

			XXXXObject xObj = new XXXXObject();
			byte[] bs = new byte[value.getLength()];
			System.arraycopy(value.getBytes(), 0, bs, 0, value.getLength());
			VipcloudUtil.DeserializeObject(bs, xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("identifier_issn")) {
					identifier_issn = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("date_created")) {
					date_created = updateItem.getValue().trim().replace("-", "");
				}
				if (updateItem.getKey().equals("identifier_standard")) {
					identifier_standard = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("date_impl")) {
					date_impl = updateItem.getValue().trim().replace("-", "");
				}
				if (updateItem.getKey().equals("creator_cluster")) {
					creator_cluster = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("creator_institution")) {
					creator_institution = updateItem.getValue().trim().replace(" ", "");
				}
				if (updateItem.getKey().equals("creator")) {
					creator = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("agency")) {
					agency = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("agents")) {
					agents = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("province_code")) {
					province_code = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("description")) {
					description = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("subject_csc")) {
					subject_csc = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("subject_isc")) {
					subject_isc = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("page")) {
					page = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("description_core")) {
					description_core = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("db")) {
					description_type = updateItem.getValue().trim();
				}

				if (updateItem.getKey().equals("legalState")) {
					legal_status = updateItem.getValue().trim();
				}
			}
			 rawid = identifier_standard;
			context.getCounter("map", "countAll").increment(1);
			
			if(legal_status.length() < 1){
				context.getCounter("map", "Not legal_status").increment(1);
				return;
			}
			if(legal_status.equals("not")){
				legal_status = "";
			}
			
			// 处理rawtype
			 if(description_type.equals("SCPD_WG")){
				 description_type = "外观设计";
			 }else if(description_type.equals("SCPD_XX")){
				 description_type = "实用新型";
			 }else if(description_type.equals("SCPD_FM")){
				 description_type = "发明专利";
			 }
			 
			lngid = VipIdEncode.getLngid(sub_db_id, rawid, false);
						
			if (title.length() < 1) {
				context.getCounter("map", "no title").increment(1);
				return;
			}
			// 剔除rawid为空的数据
			if (rawid.length() < 1) {
				context.getCounter("map", "no rawid").increment(1);
				return;
			}
			if (date_impl.length() == 8) {
				date = date_impl.substring(0,4);
				context.getCounter("map", "date_impl.len ==8 ").increment(1);

			} else {
				context.getCounter("map", "date_created.len !=8 ").increment(1);
			}

			provider_url = "http://dbpub.cnki.net/grid2008/dbpub/detail.aspx?dbname=SCPD&filename=" + rawid;


			XXXXObject xObjOut = new XXXXObject();
			{
				xObjOut.data.put("lngid", lngid);
				xObjOut.data.put("rawid", rawid);
				xObjOut.data.put("sub_db_id", "00003");
				xObjOut.data.put("product", "CNKI");
				xObjOut.data.put("sub_db", "SCPD");
				xObjOut.data.put("provider", "CNKI");
				xObjOut.data.put("down_date", "20190101");
				xObjOut.data.put("batch", "20190524_010101");
				xObjOut.data.put("source_type", "7");
				xObjOut.data.put("ipc_no", subject_isc);
				xObjOut.data.put("ipc_no_1st", subject_csc);
				xObjOut.data.put("loc_no", "");
				xObjOut.data.put("loc_no_1st", "");
				xObjOut.data.put("cpc_no", "");
				xObjOut.data.put("cpc_no_1st", "");
				xObjOut.data.put("ecla_no", "");
				xObjOut.data.put("ecla_no_1st", "");
				xObjOut.data.put("ccl_no", "");
				xObjOut.data.put("ccl_no_1st", "");
				xObjOut.data.put("fi_no", "");
				xObjOut.data.put("fi_no_1st", "");
				xObjOut.data.put("agency", agency);
				xObjOut.data.put("agent",agents.replace(",", ";"));
				xObjOut.data.put("applicant", creator_cluster.replace(",", ";"));
				xObjOut.data.put("applicant_addr", creator_institution);
				xObjOut.data.put("claim",description_core);
				xObjOut.data.put("legal_status",legal_status);
				xObjOut.data.put("pct_app_data", "");
				xObjOut.data.put("pct_enter_nation_date", "");
				xObjOut.data.put("pct_pub_data", "");
				xObjOut.data.put("priority", "");
				xObjOut.data.put("priority_date", "");
				xObjOut.data.put("priority_no", "");
				xObjOut.data.put("app_no",identifier_issn);
				xObjOut.data.put("app_date", date_created); // 申请日
				xObjOut.data.put("pub_no",identifier_standard);
				xObjOut.data.put("doi", "");
				xObjOut.data.put("provider_url", provider_url);
				xObjOut.data.put("title", title);
				xObjOut.data.put("keyword", "");
				xObjOut.data.put("clc_no_1st", "");
				xObjOut.data.put("clc_no", "");
				xObjOut.data.put("abstract",description);
				xObjOut.data.put("raw_type",description_type);
				xObjOut.data.put("pub_date", date_impl);// 公开日
				xObjOut.data.put("page_cnt", page);
				xObjOut.data.put("pdf_size", "");
				xObjOut.data.put("word_cnt", "");
				xObjOut.data.put("fulltext_txt", "");
				xObjOut.data.put("fulltext_addr", "");
				xObjOut.data.put("fulltext_type", "");
				xObjOut.data.put("fund", "");
				xObjOut.data.put("author", creator.replace(",", ";"));
				xObjOut.data.put("organ",creator_cluster.replace(",", ";"));
				xObjOut.data.put("organ_area",province_code);
				xObjOut.data.put("pub_year", date);
				xObjOut.data.put("publisher", "");
				xObjOut.data.put("cover_path", "");
				xObjOut.data.put("country", country);
				xObjOut.data.put("language", language);
				xObjOut.data.put("family_pub_no", "");
				xObjOut.data.put("ref_cnt", "");
				xObjOut.data.put("ref_id", "");
				xObjOut.data.put("cited_id", "");
				xObjOut.data.put("cited_cnt", "");
				xObjOut.data.put("down_cnt", "");

			}
			context.getCounter("map", "count").increment(1);

			byte[] bytes = VipcloudUtil.SerializeObject(xObjOut);
			context.write(new Text(rawid), new BytesWritable(bytes));
		}
	}

	public static class ProcessReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {

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
