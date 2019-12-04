package simple.jobstream.mapreduce.user.walker.wf_qk;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.vip.VipIdEncode;

// 将以往数据转为A层格式
public class Old2A extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 0;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";

	public void pre(Job job) {
		String jobName = "wf_qk." + this.getClass().getSimpleName();
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
//		private static String lngid = "";
		private static String bookid = "";
		private static String pykm = "";
		private static String issn = "";
		private static String cnno = "";
		private static String title_c = "";
		private static String title_e = "";
		private static String remark_c = "";
		private static String remark_e = "";
		private static String doi = "";
		private static String author_c = "";
		private static String author_e = "";
		private static String firstwriter = "";
		private static String showwriter = "";
		private static String cbmwriter = "";
		private static String writer = "";
		private static String organ = "";
		private static String firstorgan = "";
		private static String showorgan = "";
		private static String name_c = "";
		private static String name_e = "";
		private static String years = "";
		private static String vol = "";
		private static String num = "";
		private static String sClass = "";
		private static String firstclass = "";
		private static String auto_class = "";
		private static String keyword_c = "";
		private static String keyword_e = "";
		private static String imburse = "";
		private static String pageline = "";
		private static String pagecount = "";
		private static String ref_cnt = "";
		private static String cited_cnt = "";
		private static String beginpage = "";
		private static String endpage = "";
		private static String jumppage = "";
		private static String muinfo = "";
		private static String pub1st = "0"; // 是否优先出版
		private static String fromtype = "WANFANG";
		private static String down_date = "";
		
		public void setup(Context context) throws IOException, InterruptedException {
			initMapPykmLanguage(context);
		}

		// pykm - 语言
		private static Map<String, String> mapLanguage = new HashMap<String, String>();

		private static void initMapPykmLanguage(Context context) throws IOException {
			// 获取HDFS文件系统
			FileSystem fs = FileSystem.get(context.getConfiguration());

			FSDataInputStream fin = fs.open(new Path("/RawData/wanfang/qk/_rel_file/pykm_language.txt"));
			BufferedReader in = null;
			String line;
			String[] vec = null;
			try {
				in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
				while ((line = in.readLine()) != null) {
					line = line.trim();
					vec = line.split("★");
					if (vec.length != 2) {
						continue;
					}
					mapLanguage.put(vec[0].trim(), vec[1].trim());
				}
			} finally {
				if (in != null) {
					in.close();
				}
			}
			System.out.println("mapLanguage size: " + mapLanguage.size());
		}

		// 获取语言
		private static String getLanguage(String pykm) {
			String language = "ZH";
			if (mapLanguage.containsKey(pykm)) {
				if (mapLanguage.get(pykm).equals("eng")) {
					language = "EN";
				}
			}

			return language;
		}

		private String[] parsePageInfo(String line) {
			String beginpage = "";
			String endpage = "";
			String jumppage = "";

			int idx = line.indexOf(',');
			if (idx > 0) {
				jumppage = line.substring(idx + 1).trim();
				line = line.substring(0, idx).trim(); // 去掉加号及以后部分
			}
			idx = line.indexOf('-');
			if (idx > 0) {
				endpage = line.substring(idx + 1).trim();
				line = line.substring(0, idx).trim(); // 去掉减号及以后部分
			}
			beginpage = line.trim();
			if (endpage.length() < 1) {
				endpage = beginpage;
			}

			String[] vec = { beginpage, endpage, jumppage };
			return vec;
		}

		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
			{// 老字段
				rawid = "";
//				lngid = "";
				bookid = "";
				pykm = "";
				issn = "";
				cnno = "";
				title_c = "";
				title_e = "";
				remark_c = "";
				remark_e = "";
				doi = "";
				author_c = "";
				author_e = "";
				firstwriter = "";
				showwriter = "";
				cbmwriter = "";
				writer = "";
				organ = "";
				firstorgan = "";
				showorgan = "";
				name_c = "";
				name_e = "";
				years = "";
				vol = "";
				num = "";
				sClass = "";
				firstclass = "";
				auto_class = "";
				keyword_c = "";
				keyword_e = "";
				imburse = "";
				pageline = "";
				pagecount = "";
				ref_cnt = "";
				cited_cnt = "";
				beginpage = "";
				endpage = "";
				jumppage = "";
				muinfo = "";
				pub1st = "0"; // 是否优先出版
				fromtype = "WANFANG";
				down_date = "";
			}

			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pykm")) {
					pykm = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("issn")) {
					issn = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("cnno")) {
					cnno = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("title_c")) {
					title_c = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("title_e")) {
					title_e = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("remark_c")) {
					remark_c = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("remark_e")) {
					remark_e = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("doi")) {
					doi = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("author_c")) {
					author_c = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("author_e")) {
					author_e = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("organ")) {
					organ = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("name_c")) {
					name_c = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("name_e")) {
					name_e = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("years")) {
					years = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("vol")) {
					vol = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("num")) {
					num = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("sClass")) {
					sClass = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("auto_class")) {
					auto_class = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("keyword_c")) {
					keyword_c = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("keyword_e")) {
					keyword_e = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("imburse")) {
					imburse = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pageline")) {
					pageline = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pagecount")) {
					pagecount = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("ref_cnt")) {
					ref_cnt = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("cited_cnt")) {
					cited_cnt = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("muinfo")) {
					muinfo = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pub1st")) {
					pub1st = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("down_date")) {
					down_date = updateItem.getValue().trim();
				}
			}

			showwriter = author_c.length() > 0 ? author_c : author_e;
			showorgan = organ;
//			lngid = getLngIDByWanID(rawid);				
//			bookid = getBookId(pykm, years, num);
			sClass = sClass.replaceAll("\\s+", ";");
			String[] vec = sClass.split(";");
			if (vec.length > 0) {
				firstclass = vec[0].trim();
			}
			vec = showwriter.split(";");
			if (vec.length > 0) {
				firstwriter = vec[0].trim();
				firstwriter = firstwriter.replaceAll("\\[.*?\\]$", ""); // 去掉后面的标号
			}
			writer = cbmwriter = showwriter;
			// writer = showwriter.replace(';', ' ');

			vec = showorgan.split(";");
			if (vec.length > 0) {
				firstorgan = vec[0].trim();
				firstorgan = firstorgan.replaceAll("^\\[.*?\\]", ""); // 去掉前面的标号
			}

			vec = parsePageInfo(pageline);
			beginpage = vec[0];
			endpage = vec[1];
			jumppage = vec[2];

			imburse = imburse.replace('%', ';');

			// 补缺
			{
				if (title_c.length() < 1) {
					title_c = title_e;
				}
			}

			XXXXObject xObjOut = new XXXXObject();
			{
				xObjOut.data.put("lngid", VipIdEncode.getLngid("00004", rawid, false));
				xObjOut.data.put("rawid", rawid);
				xObjOut.data.put("sub_db_id", "00004");
				xObjOut.data.put("product", "WANFANG");
				xObjOut.data.put("sub_db", "CSPD");
				xObjOut.data.put("provider", "WANFANG");
				xObjOut.data.put("down_date", down_date);
				xObjOut.data.put("batch", "20190101_010101");
				xObjOut.data.put("doi", doi);
				xObjOut.data.put("source_type", "3");
				xObjOut.data.put("provider_url",
						"http://www.wanfangdata.com.cn/details/detail.do?_type=perio&id=" + rawid);
				xObjOut.data.put("title", title_c);
				xObjOut.data.put("title_alt", title_e);
				xObjOut.data.put("title_sub", "");
				xObjOut.data.put("title_series", "");
				xObjOut.data.put("keyword", keyword_c);
				xObjOut.data.put("keyword_alt", keyword_e);
				xObjOut.data.put("keyword_machine", "");
				xObjOut.data.put("clc_no_1st", firstclass);
				xObjOut.data.put("clc_no", sClass);
				xObjOut.data.put("clc_machine", "");
				xObjOut.data.put("subject_word", "");
				xObjOut.data.put("subject_edu", "");
				xObjOut.data.put("subject", "");
				xObjOut.data.put("abstract", remark_c);
				xObjOut.data.put("abstract_alt", remark_e);
				xObjOut.data.put("abstract_type", "");
				xObjOut.data.put("abstract_alt_type", "");
				xObjOut.data.put("page_info", pageline);
				xObjOut.data.put("begin_page", beginpage);
				xObjOut.data.put("end_page", endpage);
				xObjOut.data.put("jump_page", jumppage);
				xObjOut.data.put("doc_code", "");
				xObjOut.data.put("doc_no", "");
				xObjOut.data.put("raw_type", "");
				xObjOut.data.put("recv_date", "");
				xObjOut.data.put("accept_date", "");
				xObjOut.data.put("revision_date", "");
				xObjOut.data.put("pub_date", "");
				xObjOut.data.put("pub_date_alt", "");
				xObjOut.data.put("pub_place", "");
				xObjOut.data.put("page_cnt", pagecount);
				xObjOut.data.put("pdf_size", "");
				xObjOut.data.put("fulltext_txt", "");
				xObjOut.data.put("fulltext_addr", "");
				xObjOut.data.put("fulltext_type", "pdf");
				xObjOut.data.put("column_info", muinfo);
				xObjOut.data.put("fund", imburse);
				xObjOut.data.put("fund_alt", "");
				xObjOut.data.put("author_id", "");
				xObjOut.data.put("author_1st", firstwriter);
				xObjOut.data.put("author", showwriter);
				xObjOut.data.put("author_raw", "");
				xObjOut.data.put("author_alt", "");
				xObjOut.data.put("corr_author", "");
				xObjOut.data.put("corr_author_id", "");
				xObjOut.data.put("email", "");
				xObjOut.data.put("subject_dsa", "");
				xObjOut.data.put("research_field", "");
				xObjOut.data.put("contributor", "");
				xObjOut.data.put("contributor_id", "");
				xObjOut.data.put("contributor_alt", "");
				xObjOut.data.put("author_intro", "");
				xObjOut.data.put("organ_id", "");
				xObjOut.data.put("organ_1st", firstorgan);
				xObjOut.data.put("organ", organ);
				xObjOut.data.put("organ_alt", "");
				xObjOut.data.put("preferred_organ", "");
				xObjOut.data.put("host_organ_id", "");
				xObjOut.data.put("organ_area", "");
				xObjOut.data.put("journal_raw_id", pykm);
				xObjOut.data.put("journal_name", name_c);
				xObjOut.data.put("journal_name_alt", name_e);
				xObjOut.data.put("pub_year", years);
				xObjOut.data.put("vol", vol);
				xObjOut.data.put("num", num);
				xObjOut.data.put("is_suppl", "");
				xObjOut.data.put("issn", issn);
				xObjOut.data.put("eissn", "");
				xObjOut.data.put("cnno", cnno);
				xObjOut.data.put("publisher", "");
				xObjOut.data.put("cover_path", "");
				xObjOut.data.put("is_oa", "");
				xObjOut.data.put("country", "CN");
				xObjOut.data.put("language", getLanguage(pykm));
				xObjOut.data.put("ref_cnt", ref_cnt);
				xObjOut.data.put("ref_id", "");
				xObjOut.data.put("cited_id", "");
				xObjOut.data.put("cited_cnt", "");
				xObjOut.data.put("down_cnt", "");
				xObjOut.data.put("is_topcited", "");
				xObjOut.data.put("is_hotpaper", "");
			}
			context.getCounter("map", "count").increment(1);

			byte[] bytes = VipcloudUtil.SerializeObject(xObjOut);
			context.write(new Text(rawid), new BytesWritable(bytes));
		}
	}
}
