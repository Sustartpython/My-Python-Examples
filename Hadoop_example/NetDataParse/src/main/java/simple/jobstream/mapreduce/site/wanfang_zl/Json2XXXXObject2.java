package simple.jobstream.mapreduce.site.wanfang_zl;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.BXXXXObject;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.SimpleTextInputFormat;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class Json2XXXXObject2 extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 10;
	private static int reduceNum = 10;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = ""; // 这个目录会被删除重建

	public void pre(Job job) {
		String jobName = "Step1_html2xxxxobject";
		if (testRun) {
			jobName = "wfzl_" + jobName;
		}
		job.setJobName(jobName);

		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
		reduceNum = Integer.parseInt(job.getConfiguration().get("reduceNum"));
	}

	public String getHdfsInput() {
		return inputHdfsPath;
	}

	public String getHdfsOutput() {
		return outputHdfsPath;
	}

	public void SetMRInfo(Job job) {
		job.getConfiguration().set("io.compression.codecs",
				"org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println(job.getConfiguration().get("io.compression.codecs"));

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(UniqXXXXObjectReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		SequenceFileOutputFormat.setCompressOutput(job, false);
		if (testRun) {
			job.setNumReduceTasks(testReduceNum);
		} else {
			job.setNumReduceTasks(reduceNum);
		}
	}

	public void post(Job job) {

	}

	public String GetHdfsInputPath() {
		return inputHdfsPath;
	}

	public String GetHdfsOutputPath() {
		return outputHdfsPath;
	}

	public static class ProcessMapper extends Mapper<LongWritable, Text, Text, BytesWritable> {
		private static String rawid = "";
		private static String lngid = "";
		private static String title = "";
		private static String applicationnum = "";// 申请号
		private static String applicationdata = ""; // 申请日
		private static String media_c = "";// 公开号
		private static String opendata = "";// 公开日
		private static String showorgan = "";// 申请人
		private static String applicantaddr = "";// 申请人地址
		private static String showwriter = "";// 发明人
		private static String agency = "";// 代理机构
		private static String agents = "";// 代理人
		private static String provincecode = "";// 国省代码
		private static String remark_c = "";// 摘要
		private static String mainclass = "";// 主分类号
		private static String classnum = "";// 专利分类号
		private static String language = "ZH";
		private static String country = "CN";
		private static String provider_url = "";
		private static String provider_id = "";
		private static String type = "7";
		private static String medium = "2";
		private static String batch = "";
		private static String date = "";
		private static String owner = "";
		private static String page = "";
		private static String years = "";
		private static String sovereignty = "";// 主权项
		private static String legalstatus = "";// 法律状态
		private static String maintype = "";// 专利类型
		private static String dataString = "";
		private static String sub_db_id = "00052";
		private static String date_impl = "";
		private static String author_1st = "";
		private static String clc_no_1st = "";
		private static String raw_type = "";

		public void setup(Context context) throws IOException, InterruptedException {
			batch = context.getConfiguration().get("batch");
			context.getCounter("batch", batch).increment(1);
		}

		static int cnt = 0;

		public void parseHtml(String htmlText) {

			{
				rawid = "";
				lngid = "";
				title = "";
				applicationnum = "";// 申请号
				applicationdata = ""; // 申请日
				media_c = "";// 公开号
				opendata = "";// 公开日
				showorgan = "";// 申请人
				applicantaddr = "";// 申请人地址
				showwriter = "";// 发明人
				agency = "";// 代理机构
				agents = "";// 代理人
				provincecode = "";// 国省代码
				remark_c = "";// 摘要
				mainclass = "";// 主分类号
				classnum = "";// 专利分类号
				language = "ZH";
				country = "CN";
				provider_url = "";
				provider_id = "";
				type = "7";
				medium = "2";
				batch = "";
				date = "";
				owner = "";
				page = "";
				years = "";
				sovereignty = "";// 主权项
				legalstatus = "";// 法律状态
				maintype = "";// 专利类型
				dataString = "";
				sub_db_id = "00052";
				date_impl = "";
				author_1st = "";
				clc_no_1st = "";
				raw_type = "";

			}

			if (htmlText.split("★").length == 2) {
				String filename = htmlText.split("★")[0];
				String html = htmlText.split("★")[1].toString();
				rawid = filename;

				Document doc = Jsoup.parse(html);
				try {
					title = doc.select("title").first().text().trim();
					Element remark = doc.select("div[class=baseinfo-feild abstract]").first();
					Element remarkTag = remark.select("div[class=text]").first();
					remark_c = remarkTag.text().trim();

					// -----------------------
					// Element infoDiv = doc.select("div[class=fixed-width
					// baseinfo-feild]").first();
					Elements listItem = doc.select("div[class=row]");
					for (Element e : listItem) {
						Element item = e.select("span[class=pre]").first();
						Element value = e.select("span[class=text]").first();
						String itemText = item.text().trim();
						String valueText = value.text().trim();

						if (itemText.contains("专利类型")) {
							maintype = valueText;
						} else if (itemText.contains("申请（专利）号")) {
							applicationnum = valueText;
						} else if (itemText.contains("申请日期")) {
							applicationdata = valueText;

							applicationdata = applicationdata.replace("年", " ").replace("月", " ").replace("日", "");
							String[] openlist = applicationdata.split(" ");
							if (openlist.length == 3) {
								String n = openlist[0];
								String yue = openlist[1];
								String ri = openlist[2];
								if (yue.length() < 2) {
									yue = '0' + yue;

								}
								if (ri.length() < 2) {
									ri = '0' + ri;

								}
								date_impl = n + yue + ri;
							}
							if (openlist.length == 2) {
								String n = openlist[0];
								String yue = openlist[1];
								String ri = openlist[2];
								if (yue.length() < 2) {
									yue = '0' + yue + "00";

								}

								date_impl = n + yue;
							}
							if (openlist.length == 1) {
								String n = openlist[0];
								date_impl = n + "0000";
							}

						} else if (itemText.contains("公开(公告)日")) {
							opendata = valueText;
							opendata = opendata.replace("-", "").replace("'", "''").replace("-", "").replace("年", " ")
									.replace("月", " ").replace("日", "");
							if (!opendata.equals("")) {
								String[] list = opendata.split(" ");
								if (list.length == 3) {
									date = list[0];
									String yue = list[1];
									String ri = list[2];
									if (yue.length() < 2) {
										yue = '0' + yue;
									}
									if (ri.length() < 2) {
										ri = '0' + ri;
									}
									dataString = date + yue + ri;
								}
								if (list.length == 2) {
									date = list[0];
									String yue = list[1];
									String ri = list[2];
									if (yue.length() < 2) {
										yue = '0' + yue + "00";
									}
									dataString = date + yue;
								}
								if (list.length == 1) {
									date = list[0];
									dataString = date + "0000";
								}

							} else {
								dataString = "19000000";
								date = "1900";
							}
						} else if (itemText.contains("公开(公告)号：")) {
							media_c = valueText;
						} else if (itemText.contains("主分类号：")) {
							mainclass = valueText.replaceAll(",", ";");
							;
						} else if (itemText.startsWith("分类号：")) {
							classnum = valueText.replaceAll(",", ";");
							;
						} else if (itemText.contains("申请（专利权）人：")) {
							showorgan = valueText;
						} else if (itemText.contains("发明（设计）人：")) {
							showwriter = valueText;
						} else if (itemText.contains("主申请人地址：")) {
							applicantaddr = valueText.trim().replace(" ", "");
							;
						} else if (itemText.contains("专利代理机构：")) {
							agency = valueText;
						} else if (itemText.contains("代理人：")) {
							agents = valueText;
						} else if (itemText.contains("国别省市代码：")) {
							provincecode = valueText;
						} else if (itemText.contains("主权项：")) {
							sovereignty = valueText;
						} else if (itemText.contains("法律状态：")) {
							legalstatus = valueText;
						}
					}

				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			cnt += 1;
			if (cnt == 1) {
				System.out.println("text:" + value.toString());
			}

			parseHtml(value.toString());

			lngid = VipIdEncode.getLngid(sub_db_id, rawid, false);

			context.getCounter("map", "countAll").increment(1);
			if (title.length() < 1) {
				context.getCounter("map", "no title").increment(1);
				return;
			}
			// 剔除rawid为空的数据
			if (rawid.length() < 1) {
				context.getCounter("map", "no rawid").increment(1);
				return;
			}
			if (dataString.substring(0, 4).equals(date)) {
//				dataString = "19000000";
//				date = "1900";
				context.getCounter("map", "dataString ==date ").increment(1);

			} else {
				context.getCounter("map", "dataString !=date ").increment(1);
			}

			provider_url = "http://www.wanfangdata.com.cn/details/detail.do?_type=patent&id=" + rawid;

			{
				// 处理主分类号,
				if (mainclass.length() > classnum.length()) {
					// 选取长的作为主分类号
					classnum = mainclass;
					if (classnum.contains(";")) {
						mainclass = classnum.split(";")[0];
					} else {
						mainclass = mainclass;
					}
				} else {
					classnum = classnum;
					if (classnum.contains(";")) {
						mainclass = classnum.split(";")[0];
					} else {
						mainclass = mainclass;
					}
				}
			}
			// 统一国省代码中的;号为括号
			provincecode = provincecode.replace(";","(").replaceAll("$", ")");
			// 处理pub_date
			if (date_impl.length() == 4) {
				date_impl = date + "0000";
				context.getCounter("map", "dataString !=4").increment(1);
			}

			XXXXObject xObjOut = new XXXXObject();
			{
				xObjOut.data.put("lngid", lngid);
				xObjOut.data.put("rawid", rawid);
				xObjOut.data.put("sub_db_id", "00052");
				xObjOut.data.put("product", "WANFANG");
				xObjOut.data.put("sub_db", "WFPD");
				xObjOut.data.put("provider", "WANFANG");
				xObjOut.data.put("down_date", "20190618");
				xObjOut.data.put("batch", batch);
				xObjOut.data.put("source_type", "7");
				xObjOut.data.put("ipc_no", classnum);
				xObjOut.data.put("ipc_no_1st", mainclass);
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
				xObjOut.data.put("agent", agents.replace(",", ";"));
				xObjOut.data.put("applicant", showorgan.replace(",", ";"));
				xObjOut.data.put("applicant_addr", applicantaddr);
				xObjOut.data.put("claim", sovereignty);
				xObjOut.data.put("legal_status", legalstatus);
				xObjOut.data.put("pct_app_data", "");
				xObjOut.data.put("pct_enter_nation_date", "");
				xObjOut.data.put("pct_pub_data", "");
				xObjOut.data.put("priority", "");
				xObjOut.data.put("priority_date", "");
				xObjOut.data.put("priority_no", "");
				xObjOut.data.put("app_no", applicationnum);
				xObjOut.data.put("app_date", date_impl); // 申请日
				xObjOut.data.put("pub_no", media_c);
				xObjOut.data.put("doi", "");
				xObjOut.data.put("provider_url", provider_url);
				xObjOut.data.put("title", title);
				xObjOut.data.put("keyword", "");
				xObjOut.data.put("clc_no_1st", "");
				xObjOut.data.put("clc_no", "");
				xObjOut.data.put("abstract", remark_c);
				xObjOut.data.put("raw_type", maintype);
				xObjOut.data.put("pub_date", dataString);// 公开日
				xObjOut.data.put("page_cnt", page);
				xObjOut.data.put("pdf_size", "");
				xObjOut.data.put("word_cnt", "");
				xObjOut.data.put("fulltext_txt", "");
				xObjOut.data.put("fulltext_addr", "");
				xObjOut.data.put("fulltext_type", "");
				xObjOut.data.put("fund", "");
				xObjOut.data.put("author", showwriter.replace(",", ";"));
				xObjOut.data.put("organ", showorgan.replace(",", ";"));
				xObjOut.data.put("organ_area", provincecode);
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

}
