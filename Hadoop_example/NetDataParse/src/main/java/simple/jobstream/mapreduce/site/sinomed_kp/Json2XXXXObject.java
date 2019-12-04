package simple.jobstream.mapreduce.site.sinomed_kp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;
import simple.jobstream.mapreduce.common.vip.AuthorOrgan;
import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;
import simple.jobstream.mapreduce.site.cssci.Json2XXXXObject.ProcessMapper;
import simple.jobstream.mapreduce.common.util.DateTimeHelper;

//统计wos和ei的数据量
public class Json2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 50;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";

	public void pre(Job job) {
		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
		reduceNum = Integer.parseInt(job.getConfiguration().get("reduceNum"));
		job.setJobName(job.getConfiguration().get("jobName"));

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
		job.setReducerClass(UniqXXXXObjectReducer.class);

		TextOutputFormat.setCompressOutput(job, false);

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

	// ======================================处理逻辑=======================================
	// 继承Mapper接口,设置map的输入类型为<Object,Text>
	// 输出类型为<Text,IntWritable>
	public static class ProcessMapper extends Mapper<LongWritable, Text, Text, BytesWritable> {
		public String batch = "";

		public void setup(Context context) throws IOException, InterruptedException {
			batch = context.getConfiguration().get("batch");
		}

		public static String stdDate(String date) {
			String year = "";
			String month = "";
			String day = "";
			Matcher datetype1 = Pattern.compile("(\\d{1,2})[\\s,\\,]+([A-Za-z]+)[\\s,\\,]+(\\d{4})").matcher(date);
			Matcher datetype2 = Pattern.compile("([A-Za-z]+)[\\s,\\,]+(\\d{1,2})[\\s,\\,]+(\\d{4})").matcher(date);
			Matcher datetype3 = Pattern.compile("([A-Za-z]+)[\\s,\\,]+(\\d{4})").matcher(date);
			if (datetype1.find()) {
				day = datetype1.group(1);
				month = datetype1.group(2);
				year = datetype1.group(3);
			} else if (datetype2.find()) {
				day = datetype2.group(2);
				month = datetype2.group(1);
				year = datetype2.group(3);
			} else if (datetype3.find()) {
				day = "00";
				month = datetype3.group(1);
				year = datetype3.group(2);
			} else {
				return date;
			}

			if (day.length() == 1) {
				day = "0" + day;
			}
			month = month.toLowerCase();
			if (month.startsWith("jan")) {
				month = "01";
			} else if (month.startsWith("feb")) {
				month = "02";
			} else if (month.startsWith("mar")) {
				month = "03";
			} else if (month.startsWith("apr")) {
				month = "04";
			} else if (month.startsWith("may")) {
				month = "05";
			} else if (month.startsWith("jun")) {
				month = "06";
			} else if (month.startsWith("jul")) {
				month = "07";
			} else if (month.startsWith("aug")) {
				month = "08";
			} else if (month.startsWith("sep")) {
				month = "09";
			} else if (month.startsWith("oct")) {
				month = "10";
			} else if (month.startsWith("nov")) {
				month = "11";
			} else if (month.startsWith("dec")) {
				month = "12";
			} else if (month.startsWith("spring")) {
				month = "03";
			} else if (month.startsWith("summer")) {
				month = "06";
			} else if (month.startsWith("autumn")) {
				month = "09";
			} else if (month.startsWith("fall")) {
				month = "09";
			} else if (month.startsWith("winter")) {
				month = "12";
			} else {
				return year + "0000";
			}

			return year + month + day;
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			Gson gson = new Gson();
			Type type = new TypeToken<Map<String, String>>() {
			}.getType();
			Map<String, String> mapJson = null;
			try {
				mapJson = gson.fromJson(value.toString(), type);
			} catch (Exception e) {
				// TODO: handle exception
				return;
			}

			String journal_raw_id = mapJson.get("journal_id").trim();
			String journal_name = mapJson.get("journal_name").trim();
			String publisher = mapJson.get("publisher").trim();
			String issn = mapJson.get("issn").trim();
			String cnno = mapJson.get("cn").trim();
			String provider_url = mapJson.get("article_url").trim();
			String title = mapJson.get("article_name").trim().replace("\n", "");
			String html = mapJson.get("htmlText").trim();

			Document doc = Jsoup.parse(html);

			LinkedHashMap<String, String> mapcre_ins = new LinkedHashMap<String, String>();

			String information = "";
			String author = "";
			String organ = "";
			String message = "";
			String abstract_ = "";
			String pub_place = "";
			String pub_year = "";
			String pub_date = "";
			String vol = "";
			String num = "";
			String page_info = "";
			String begin_page = "";
			String end_page = "";
			String clc_no = "";
			String clc_no_1st = "";
			String subject_word = "";
			String keyword = "";
			String rawid = "";

			String down_date = "20191112";

			String lngid = "";
			String product = "SINOMED";
			String sub_db = "KPQK";
			String sub_db_id = "00187";
			String provider = "sinomedkpjournal";
			String source_type = "3";
			String country = "CN";
			String language = "ZH";
			rawid = provider_url.replace("http://www.sinomed.ac.cn/kp/detail.do?ui=", "");
			Elements div = doc.select("div.lab-txt.fL.w100");
			for (Element p : div) {
				for (Element pTag : p.select("p")) {
					information = pTag.text().trim();

//					if (information.startsWith("流水号:")) {
//						rawid = information.replace("流水号:", "").trim();
//						if (rawid.equals("")) {
//							rawid = provider_url.replace("http://www.sinomed.ac.cn/kp/detail.do?ui=", "");
//						}
//					}
					if (information.startsWith("作者:")) {
						author = information.replace("作者:", "").trim();
						author = author.replace("(", "[").replace(")", "]").replace("  ", "").replace(" ", "");
					}
					if (information.startsWith("作者单位:")) {
						organ = information.replace("作者单位:", "").trim();
						organ = organ.replace("(", "[").replace(")", "]").replace("  ", "").replace(" ", "");
						if (organ.equals("不详")) {
							organ = "";
						}
					}
					if (!author.equals("") && !author.contains("[") && !organ.equals("")) {
						for (int a = 0; a < author.split(";").length; a++) {
							mapcre_ins.put(author.split(";")[a].replace("  ", ""), organ);
						}
						String[] result = AuthorOrgan.numberByMap(mapcre_ins);
						author = result[0];
						organ = result[1];
					}
					if (information.startsWith("出处:")) {
						message = information.replace("出处:", "").trim();
						String[] temp = message.split("  ");
						String[] temp2 = temp[1].split("; ");
						pub_date = temp2[0];
						pub_date = pub_date.replace(".", "");
						if (pub_date.length() == 6) {
							pub_date = pub_date + "00";
						} else if (pub_date.length() == 4) {
							pub_date = pub_date + "0000";
						}
						String[] temp3 = temp2[1].split(" : ");
						final String regex = "(\\(\\d+\\))";
						final String string = temp3[0];

						final Pattern pattern = Pattern.compile(regex, Pattern.MULTILINE);
						final Matcher matcher = pattern.matcher(string);
						while (matcher.find()) {
							num = matcher.group(0);
						}
						vol = string.replace(num, "");
						num = num.replace("(", "").replace(")", "");

						if (temp3.length == 2) {
							page_info = temp3[1];
							if (!page_info.contains(",")) {
								if (page_info.contains("-")) {
									begin_page = page_info.split("-")[0];
									end_page = page_info.split("-")[1];
								}
							}
						}

					}
					if (information.startsWith("摘要: ")) {
						abstract_ = information.replace("摘要: ", "").trim();
					}
					if (information.startsWith("出版地:")) {
						pub_place = information.replace("出版地:", "").trim();
					}
					if (information.startsWith("分类号:")) {
						clc_no = information.replace("分类号:", "").trim().replace("  ", "");
						if (clc_no.contains(";")) {
							clc_no_1st = clc_no.split(";")[0];
						}
					}
					if (information.startsWith("主题词:")) {
						subject_word = information.replace("主题词:", "").trim().replace(" ", "").replace("*", "");;
					}
					if (information.startsWith("关键词:")) {
						keyword = information.replace("关键词:", "").trim().replace("*", "").replace("  ", "");
					}
					if (information.startsWith("特征词:")) {
						if (subject_word.equals("")) {
							subject_word = information.replace("特征词:", "").trim().replace(" ", "").replace("*", "");;
						} else {
							subject_word = subject_word + ";"
									+ information.replace("特征词:", "").trim().replace("  ", "").replace("*", "");
						}
					}
				}
			}
			if (!pub_date.equals("")) {
				pub_year = pub_date.substring(0, 4).trim();
			} else {
				pub_year = "1900";
				pub_date = "19000000";
			}

			Date dt = new Date();
			DateFormat df = new SimpleDateFormat("yyyyMMdd");
			String nowDate = df.format(dt);
			batch = nowDate + "00";
			lngid = VipIdEncode.getLngid(sub_db_id, rawid, false);

			XXXXObject xObj = new XXXXObject();

			xObj.data.put("journal_raw_id", journal_raw_id);
			xObj.data.put("journal_name", journal_name);
			xObj.data.put("publisher", publisher);
			xObj.data.put("issn", issn);
			xObj.data.put("cnno", cnno);
			xObj.data.put("provider_url", provider_url);
			xObj.data.put("title", title);
			xObj.data.put("author", author);
			xObj.data.put("organ", organ);
			xObj.data.put("abstract", abstract_);
			xObj.data.put("pub_place", pub_place);
			xObj.data.put("pub_year", pub_year);
			xObj.data.put("pub_date", pub_date);
			xObj.data.put("num", num);
			xObj.data.put("vol", vol);
			xObj.data.put("page_info", page_info);
			xObj.data.put("begin_page", begin_page);
			xObj.data.put("end_page", end_page);
			xObj.data.put("clc_no", clc_no);
			xObj.data.put("clc_no_1st", clc_no_1st);
			xObj.data.put("subject_word", subject_word);
			xObj.data.put("keyword", keyword);
			xObj.data.put("rawid", rawid);
			xObj.data.put("product", product);
			xObj.data.put("sub_db", sub_db);
			xObj.data.put("provider", provider);
			xObj.data.put("sub_db_id", sub_db_id);
			xObj.data.put("source_type", source_type);
			xObj.data.put("provider_url", provider_url);
			xObj.data.put("country", country);
			xObj.data.put("language", language);
			xObj.data.put("down_date", down_date);
			xObj.data.put("lngid", lngid);
			xObj.data.put("batch", batch);

			context.getCounter("map", "count").increment(1);

			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(rawid), new BytesWritable(bytes));

		}

	}
}