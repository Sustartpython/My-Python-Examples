package simple.jobstream.mapreduce.site.wanfang_zl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.*;
import java.util.Map;
import java.util.HashMap;

import org.apache.commons.codec.binary.Base32;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import simple.jobstream.mapreduce.common.vip.SqliteReducer;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import simple.jobstream.mapreduce.common.util.StringHelper;

import com.almworks.sqlite4java.SQLiteConnection;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.BXXXXObject;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.JobConfUtil;
import com.process.frame.util.SimpleTextInputFormat;
import com.process.frame.util.VipcloudUtil;
// 将A层xxx生成智图db
//输入应该为去重后的html
public class countXXXXobjct extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger.getLogger(StdXXXXObject.class);

	private static boolean testRun = false;
	private static int testReduceNum = 1;
	private static int reduceNum = 1;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";
	private static String postfixDb3 = "wanfangpatent";

	public void pre(Job job) {
		String jobName = "wanfangpatent.StdXXXXObject";
		

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
		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.9f);
		System.out.println("******mapred.reduce.slowstart.completed.maps*******" + job.getConfiguration().get("mapred.reduce.slowstart.completed.maps"));
		job.getConfiguration().set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputValueClass(BytesWritable.class);
		JobConfUtil.setTaskPerReduceMemory(job, 6144);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(SqliteReducer.class);

		TextOutputFormat.setCompressOutput(job, false);

		if (testRun) {
			job.setNumReduceTasks(testReduceNum);
		} else {
			job.setNumReduceTasks(reduceNum);
		}
	}

	// ======================================处理逻辑=======================================
	public static class ProcessMapper extends
			Mapper<Text, BytesWritable, Text, NullWritable> {

		public void setup(Context context) throws IOException,
				InterruptedException {

		}

		public void cleanup(Context context) throws IOException,
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
			vipID = "W_ZL_"+ vipID;
			return vipID;
		}
		public static String encodeID(String strRaw) {
			Base32 base32 = new Base32();
			String strEncode = "";
			try {
				strEncode = base32.encodeAsString(strRaw.getBytes("utf8"));
				if (strEncode.endsWith("======")) {
					strEncode = strEncode.substring(0, strEncode.length() - 6) + "0";
				} else if (strEncode.endsWith("====")) {
					strEncode = strEncode.substring(0, strEncode.length() - 4) + "1";
				} else if (strEncode.endsWith("===")) {
					strEncode = strEncode.substring(0, strEncode.length() - 3) + "8";
				} else if (strEncode.endsWith("=")) {
					strEncode = strEncode.substring(0, strEncode.length() - 1) + "9";
				}
				strEncode = StringHelper.makeTrans("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ",
						"ZYXWVUTSRQPONMLKJIHGFEDCBA9876543210", strEncode);
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return strEncode;
		}
		public static String getLngid(String sub_db_id, String rawid, boolean case_insensitive) {
			String uppercase_rawid = ""; // 大写版 rawid
			if (case_insensitive) { // 源网站的 rawid 区分大小写
				char[] rawlist = rawid.toCharArray();
				for (char ch : rawlist) {
					if (Character.toUpperCase(ch) == ch) {
						uppercase_rawid += ch;
					} else {
						uppercase_rawid += Character.toUpperCase(ch) + "_";
					}
				}
			} else {
				uppercase_rawid = rawid.toUpperCase();
			}
			String limited_id = uppercase_rawid;
			if (limited_id.length() > 20) {
				limited_id = DigestUtils.md5Hex(uppercase_rawid).toUpperCase();
			} else {
				limited_id = encodeID(uppercase_rawid);
			}
			String lngid = sub_db_id + limited_id;
			return lngid;
		}

		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			String rawid = "";
			String lngid = "";
			String title = "";
			String applicationnum = "";// 申请号
			String applicationdata = ""; // 申请日
			String media_c = "";// 公开号
			String opendata = "";// 公开日
			String showorgan = "";// 申请人
			String applicantaddr = "";// 申请人地址
			String showwriter = "";// 发明人
			String agency = "";// 代理机构
			String agents = "";// 代理人
			String provincecode = "";// 国省代码
			String remark_c = "";// 摘要
			String mainclass = "";// 主分类号
			String classnum = "";// 专利分类号
			String language = "ZH";
			String country = "CN";
			String provider = "wanfangpatent";
			String provider_url = "";
			String provider_id = "";
			String type = "7";
			String medium = "2";
			String batch = "";
			String date = "";
			String owner = "";
			String page = "";
			String years = "";
			String sovereignty = "";// 主权项
			String legalstatus = "";// 法律状态
			String maintype = "";// 专利类型
			String dataString = "";
			String sub_db_id = "00052";
			String date_impl = "";

			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();

				}
			if (!rawid.contains("QJQJQJQJQJQJ")) {context.getCounter("map", "count").increment(1);}
			

			}
		}

	}}