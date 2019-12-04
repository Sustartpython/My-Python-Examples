package simple.jobstream.mapreduce.site.chaoxingjournal;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hdfs.protocol.datatransfer.ReplaceDatanodeOnFailure;
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
import org.jsoup.select.Elements;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.util.StringHelper;
import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;

//统计wos和ei的数据量
public class Json2XXXXObject extends InHdfsOutHdfsJobInfo {

	private static int reduceNum = 10;
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

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			Gson gson = new Gson();
			Type type = new TypeToken<Map<String, String>>() {
			}.getType();
			Map<String, String> mapJson = null;
			try {
				mapJson = gson.fromJson(value.toString(), type);
			} catch (Exception e) {
				context.getCounter("map", "read_json_error_count").increment(1);
				return;
				// TODO: handle exception
			}
			

			String url = mapJson.get("url").trim();
			String creator = mapJson.get("creator").trim();
			String date = mapJson.get("date").trim();
			String dxid = mapJson.get("dxid").trim();
			String issn = mapJson.get("issn").trim();
			String volume = mapJson.get("volume").trim();
			String issue = mapJson.get("issue").trim();
			String title = mapJson.get("title").trim();
			String source = mapJson.get("source").trim();
			String publisher = mapJson.get("publisher").trim();
			String cnno = mapJson.get("cnno").trim();
			String provider_subject = mapJson.get("provider_subject").trim();
			String journal_id = mapJson.get("mags").trim();

			String down_date = "";
			if (mapJson.containsKey("down_date")) {
				down_date = mapJson.get("down_date").trim();
			}

			String html = mapJson.get("detail").trim();

			String description_source = "";
			String date_created = "";
			String subject = "";
			String contributor = "";
			String description = "";
			String doi = "";
			String beginpage = "";
			String endpage = "";
			String jumppage = "";
			String insitution = "";
			String eissn = "";
			String pissn = "";
			String fund = "";
			String cited_cnt = "";
			String clc_no = "";
			String clc = "";
			String journal_name_alt = "";
			
			String pub_year = "";
			
			String lngid = "";
			String rawid = "";
			String product = "CHAOXING";
			String sub_db = "QK";
			String provider = "CHAOXING";
			String sub_db_id = "00006";
			String source_type = "3";
			String provider_url = "http://qikan.chaoxing.com";
			String country = "CN";
			String language = "ZH";

			if (!html.equals("")) {
				Document doc = Jsoup.parse(html);
				Element titleTag = doc.select("div.F_mainright.fl > h1").first();
				if (titleTag != null) {
					titleTag.select("sup").remove();
					title = titleTag.text().trim();
				}

				Element authorTag = doc.select("div.F_mainright.fl > p").first();
				if (authorTag != null) {
					creator = authorTag.text().trim();
				}
				Elements aTags = doc.select("div.sTopImg > p > a");
				if (aTags.size() == 3) {
					journal_name_alt = aTags.get(1).text().trim().replace("《", "").replace("》", "");
					if (source.equals(journal_name_alt)) {
						journal_name_alt = "";
					}
				}

				for (Element element : doc.select("div.Fmian1 > table > tbody > tr")) {
					Element centerTag = element.select("td[align*=center]").first();
					Element leftTag = element.select("td[align*=left]").first();
					if (centerTag == null || leftTag == null) {
						continue;
					}
					String center = centerTag.text().trim();
					String left = leftTag.text().trim();
					if (center.startsWith("【作者机构】")) {
						insitution = left.replace("；", ";");
					} else if (center.startsWith("【来    源】")) {
						Matcher pagetype1 = Pattern.compile("P(\\d+)-(\\d+)页").matcher(left);
						Matcher pagetype2 = Pattern.compile("P(\\d+)-(\\d+)，(\\d+)页").matcher(left);
						if (pagetype1.find()) {
							beginpage = pagetype1.group(1);
							endpage = pagetype1.group(2);
						} else if (pagetype2.find()) {
							beginpage = pagetype2.group(1);
							endpage = pagetype2.group(2);
							jumppage = pagetype2.group(3);
						} else {
							Element divTag = doc.select("div.sTopImg").first();
							if (divTag != null) {
								left = left.replace(divTag.text().trim(), "");
								Matcher pagetype3 = Pattern.compile("(\\d+)-(\\d+)页").matcher(left);
								if (pagetype3.find()) {
									beginpage = pagetype3.group(1);
									endpage = pagetype3.group(2);
								}
							}
						}
					} else if (center.startsWith("【分 类 号】")) {
						clc_no = left.replace("；", ";").replace("|", "");
					} else if (center.startsWith("【分类导航】")) {
						clc = left.replace("->", ";");
					} else if (center.startsWith("【关 键 词】")) {
						subject = left.replaceAll("\\p{Z}+", ";");
					} else if (center.startsWith("【基    金】")) {
						fund = left.replace("；", ";");
					} else if (center.startsWith("【摘    要】")) {
						description = left;
					} else if (center.startsWith("【统计数据】")) {
						if (left.startsWith("被引量：")) {
							cited_cnt = left.replaceAll("被引量：(\\d+).*", "$1").trim();
						}
					}
				}
			}

			title = title.replaceAll("<sup>.*?</sup>", "").replaceAll("<sub>(.*?)</sub>", "$1");
			creator = creator.replaceAll("<sup>.*?</sup>", "").replaceAll("<sub>(.*?)</sub>", "$1").replace("●", "")
					.replace("，", ";").replace("■ ", "");
			volume = volume.replace("Vol.", "");
			issue = issue.replaceAll("第(\\w+)期", "$1").replace("No.", "");

			provider_subject = StringHelper.cleanSemicolon(provider_subject);
			creator = StringHelper.cleanSemicolon(creator);
			
			pub_year = date;
			rawid = dxid;
			lngid = VipIdEncode.getLngid("00006",rawid,false);
			provider_url = provider_url + url;


			XXXXObject xObj = new XXXXObject();

//			xObj.data.put("url", url);
			xObj.data.put("author", creator);
			xObj.data.put("pub_date", date + "0000");
			xObj.data.put("issn", issn);
			xObj.data.put("vol", volume);
			xObj.data.put("num", issue);
			xObj.data.put("title", title);
			xObj.data.put("journal_name", source);
			xObj.data.put("publisher", publisher);
			xObj.data.put("cnno", cnno);
			xObj.data.put("subject", provider_subject);
			xObj.data.put("organ", insitution);
			xObj.data.put("begin_page", beginpage);
			xObj.data.put("end_page", endpage);
			xObj.data.put("jump_page", jumppage);
			xObj.data.put("clc_no", clc_no);
			xObj.data.put("clc", clc);
			xObj.data.put("keyword", subject);
			xObj.data.put("fund", fund);
			xObj.data.put("abstract", description);
			xObj.data.put("cited_cnt", cited_cnt);
			xObj.data.put("journal_id", journal_id);
			xObj.data.put("journal_name_alt", journal_name_alt);
			xObj.data.put("down_date", down_date);
			xObj.data.put("batch", batch);
			
			xObj.data.put("pub_year",pub_year);			
			xObj.data.put("lngid",lngid);
			xObj.data.put("rawid",rawid);
			xObj.data.put("product",product);
			xObj.data.put("sub_db",sub_db);
			xObj.data.put("provider",provider);
			xObj.data.put("sub_db_id",sub_db_id);
			xObj.data.put("source_type",source_type);
			xObj.data.put("provider_url",provider_url);
			xObj.data.put("country",country);
			xObj.data.put("language",language);

			context.getCounter("map", "count").increment(1);

			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(dxid), new BytesWritable(bytes));

		}

	}

}