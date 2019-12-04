package simple.jobstream.mapreduce.site.mirrordqlibckrdjournal;

import java.io.IOException;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.RegEx;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.cloudera.io.netty.handler.codec.http.HttpContentEncoder.Result;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;
import simple.jobstream.mapreduce.common.vip.AuthorOrgan;
import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.util.StringHelper;

//统计wos和ei的数据量
public class Json2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 40;
	private static int reduceNum = 40;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";

	public void pre(Job job) {
		String jobName = "mirrordqlibckrdjournal." + this.getClass().getSimpleName();

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
		job.getConfiguration().set("io.compression.codecs",
				"org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(ProcessReducer.class);

		TextOutputFormat.setCompressOutput(job, false);

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

	// ======================================处理逻辑=======================================
	// 继承Mapper接口,设置map的输入类型为<Object,Text>
	// 输出类型为<Text,IntWritable>
	public static class ProcessMapper extends Mapper<LongWritable, Text, Text, BytesWritable> {

		public void setup(Context context) throws IOException, InterruptedException {
		}

		static String cleanSpace(String text) {
			text = text.replaceAll("[\\s\\p{Zs}]+", " ").trim();
			return text;
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			Gson gson = new Gson();
			Type type = new TypeToken<Map<String, String>>() {
			}.getType();
			Map<String, String> mapJson = gson.fromJson(value.toString(), type);

			String pykm = mapJson.get("pykm").toString();
			String school_type = mapJson.get("shcool_type").toString();
			String pubyear = mapJson.get("pubyear").toString();
			String rool_num = mapJson.get("num").toString();
			String journal_name = mapJson.get("jname").toString();

			String htmlText = mapJson.get("html").toString();

			String lngid = "";
			String rawid = "";
			String title = "";
			String creator_release = "";
			String issue = rool_num;
			String language = "ZH";
			String provider = "mirrordqlibckrdjournal";
			String provider_id = "";
			String provider_url = "";
			String country = "CN";
			String identifier_pissn = "";
			String types = "3";
			String source = journal_name;
			String date_created = pubyear + "0000";
			String gch = "mirrordqlibckrdjournal" + "@" + school_type + "_" + pykm;
			String publisher = "";
			String description_cycle = "";
			String date = pubyear;
			String medium = "2";
			String down_date = "20181203";
			String parse_time = (new SimpleDateFormat("yyyyMMdd")).format(new Date());

			if (htmlText.contains("box_left")) {

				Document doc = Jsoup.parse(htmlText);
				//
				Element MessagesElement = doc.select("div[id='lefta_left']").first();

				if (MessagesElement != null) {

					Elements MessageElement = doc.select("dd");

					for (Element Message : MessageElement) {

						String Message_str = Message.text().trim();

						if (Message_str.startsWith("编辑出版：")) {

							publisher = Message_str.replace("编辑出版：", "");

						} else if (Message_str.startsWith("主办单位：")) {
							creator_release = Message_str.replace("主办单位：", "");

						} else if (Message_str.startsWith("出版周期：")) {

							description_cycle = Message_str.replace("出版周期：", "");

						}

						else if (Message_str.startsWith("I S S N ：")) {
							identifier_pissn = Message_str.replace("I S S N ：", "");

						}
					}
					Element newsElement = doc.select("div[class='content']").first();

					if (newsElement != null) {

						// 获取当前标签下的所有a标签
						Elements news_aElement = newsElement.select("a");

						for (Element news_a : news_aElement) {
							// http://10.38.48.183:8013/CKRD/ckrd/home/download/?type=primary&cd=SOCI1411SS&filename=XCZW201430001

							// 获取当前标签url
							String url_str = news_a.attr("href");

							String deal_url_str = url_str.split("=")[url_str.split("=").length - 1];

							// 生成title
							title = news_a.text().trim();

							// 生成rawid
							rawid = school_type + "_" + deal_url_str;

							// 生成lngid
							lngid = "MIRROR_DQLIB_CKRD_QK_" + rawid;

							// provider_id
							provider_id = "mirrordqlibckrdjournal" + "@" + rawid;

							// provider_url
							provider_url = "mirrordqlibckrdjournal" + "@"+"http://10.38.48.183:8013" + url_str;

							if (title.length() == 0) {
								context.getCounter("map", "not find title").increment(1);
								continue;
								
							}

							if (rawid.length() == 0) {
								context.getCounter("map", "not find rawid").increment(1);
								continue;
							}

							if (rawid.length() != 0 && title.length() != 0) {
								XXXXObject xObj = new XXXXObject();
								xObj.data.put("rawid", rawid);
								xObj.data.put("lngid", lngid);
								xObj.data.put("title", title);
								xObj.data.put("creator_release", creator_release);
								xObj.data.put("issue", issue);
								xObj.data.put("language", language);
								xObj.data.put("provider", provider);
								xObj.data.put("provider_id", provider_id);
								xObj.data.put("provider_url", provider_url);
								xObj.data.put("country", country);
								xObj.data.put("identifier_pissn", identifier_pissn);
								xObj.data.put("types", types);
								xObj.data.put("source", source);
								xObj.data.put("date_created", date_created);
								xObj.data.put("gch", gch);
								xObj.data.put("publisher", publisher);
								xObj.data.put("description_cycle", description_cycle);
								xObj.data.put("date", date);
								xObj.data.put("medium", medium);
								xObj.data.put("down_date", down_date);
								xObj.data.put("parse_time", parse_time);
								xObj.data.put("school_type", school_type);

								byte[] bytes = VipcloudUtil.SerializeObject(xObj);
								context.write(new Text(rawid), new BytesWritable(bytes));
								context.getCounter("map", "count").increment(1);
							}

						}
					}
				}

			}

		}

	}

	// 继承Reducer接口，设置Reduce的输入类型为<Text,IntWritable>
	// 输出类型为<Text,IntWritable>
	public static class ProcessReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
		public void reduce(Text key, Iterable<BytesWritable> values, Context context)
				throws IOException, InterruptedException {

			BytesWritable bOut = new BytesWritable(); // 用于最后输出
			for (BytesWritable item : values) {
				if (item.getLength() > bOut.getLength()) { // 选最大的一个
					bOut = item;
				}
			}

			context.getCounter("reduce", "count").increment(1);

			context.write(key, bOut);
		}
	}
}