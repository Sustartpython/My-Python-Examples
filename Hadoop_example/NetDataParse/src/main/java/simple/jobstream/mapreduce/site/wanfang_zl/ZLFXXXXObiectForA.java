package simple.jobstream.mapreduce.site.wanfang_zl;

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

	public static String inputHdfsPath = "/RawData/wanfang/zl/latest";
	public static String outputHdfsPath = "/RawData/wanfang/zl/latestA";

	public void pre(Job job) {
		String jobName = "wanfang_zl." + this.getClass().getSimpleName();
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

			XXXXObject xObj = new XXXXObject();
			byte[] bs = new byte[value.getLength()];
			System.arraycopy(value.getBytes(), 0, bs, 0, value.getLength());
			VipcloudUtil.DeserializeObject(bs, xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("title_c")) {
					title = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("applicationnum")) {
					applicationnum = updateItem.getValue().trim();
					rawid = applicationnum;
				} else if (updateItem.getKey().equals("applicationdata")) {
					applicationdata = updateItem.getValue().trim();
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
				} else if (updateItem.getKey().equals("media_c")) {
					media_c = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("opendata")) {
					String open = updateItem.getValue().trim().replace("-", "");
					opendata = open.replace("'", "''").replace("-", "").replace("年", " ").replace("月", " ").replace("日",
							"");
				} else if (updateItem.getKey().equals("showorgan")) {
					showorgan = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("applicantaddr")) {
					applicantaddr = updateItem.getValue().trim().replace(" ", "");
				} else if (updateItem.getKey().equals("showwriter")) {
					showwriter = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("agency")) {
					agency = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("agents")) {
					agents = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("provincecode")) {
					provincecode = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("remark_c")) {
					remark_c = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("mainclass")) {
					mainclass = updateItem.getValue().trim();
					mainclass = mainclass.replaceAll(",", ";");
				} else if (updateItem.getKey().equals("classnum")) {
					classnum = updateItem.getValue().trim();
					classnum = classnum.replaceAll(",", ";");
				} else if (updateItem.getKey().equals("sovereignty")) {
					sovereignty = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("maintype")) {
					maintype = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("legalstatus")) {
					legalstatus = updateItem.getValue().trim();
				}
			}
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
				xObjOut.data.put("down_date", "20190518");
				xObjOut.data.put("batch", "20190521_010101");
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
