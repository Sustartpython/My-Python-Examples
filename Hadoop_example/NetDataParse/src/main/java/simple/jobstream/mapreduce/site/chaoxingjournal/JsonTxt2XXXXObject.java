package simple.jobstream.mapreduce.site.chaoxingjournal;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

//统计wos和ei的数据量
public class JsonTxt2XXXXObject extends InHdfsOutHdfsJobInfo {

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

		private static Map<String, String> utmap = new HashMap<String, String>();

		private static void initArrayList(Context context) throws IOException {
			FileSystem fs = FileSystem.get(context.getConfiguration());

			FSDataInputStream fin = fs.open(new Path("/user/lqx/chaoxingjournal/chaoxingjournal.txt"));

			BufferedReader reader = null;
			try {
				reader = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
				String temp;

				while ((temp = reader.readLine()) != null) {
					String[] vec = temp.split("\t");
					if (vec.length != 2) {
						continue;
					}
					utmap.put(vec[0], vec[1]);
				}

			} catch (IOException e) {
				// TODO Auto-generated catch block
			} finally {
				if (reader != null) {
					reader.close();
				}
			}
			System.out.println("*******utsetsize:" + utmap.size());

		}

		public void setup(Context context) throws IOException, InterruptedException {
			initArrayList(context);
			batch = context.getConfiguration().get("batch");

		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			Gson gson = new Gson();
			Type type = new TypeToken<Map<String, String>>() {
			}.getType();
			Map<String, String> mapJson = gson.fromJson(value.toString(), type);

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
			String mags = utmap.get(dxid);

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


			XXXXObject xObj = new XXXXObject();

			xObj.data.put("url", url);
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
			xObj.data.put("journal_id", mags);
			xObj.data.put("journal_name_alt", journal_name_alt);
			xObj.data.put("down_date", down_date);
			xObj.data.put("batch", batch);

			context.getCounter("map", "count").increment(1);

			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(dxid), new BytesWritable(bytes));

		}

	}

}