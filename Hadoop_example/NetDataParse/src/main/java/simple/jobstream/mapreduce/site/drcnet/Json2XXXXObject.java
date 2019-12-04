package simple.jobstream.mapreduce.site.drcnet;

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
			Map<String, String> mapJson = gson.fromJson(value.toString(), type);

			String leafid = mapJson.get("leafid").trim();
			String docid = mapJson.get("docid").trim();
			String down_date = mapJson.get("down_date").trim();
			String html = mapJson.get("detail").trim();

			Document doc = Jsoup.parse(html);

			String creator = "";
			String title = "";
			String description_source = "";
			String date_created = "";
			String subject = "";
			String contributor = "";
			String provider_subject = "";
			String description = "";

			Element titleTag = doc.select("#docSubject").first();
			if (titleTag != null) {
				title = titleTag.text().replace("[国研专稿]", "").replace("&amp;", "&").trim();
			}
			if (title.equals("")) {
				context.getCounter("map", "error title").increment(1);
				return;
			}
			Element authorTag = doc.select("#docAuthor").first();
			if (authorTag != null) {
				creator = authorTag.text().replace("作者：", "").trim();
				creator = creator.replaceAll("([^a-zA-Z])\\s+", "$1;").replaceAll("\\s+([^a-zA-Z])", ";$1")
						.replace("，", ";").replace("、", ";").replace(",", ";");
			}
			Element sourceTag = doc.select("#docSource").first();
			if (sourceTag != null) {
				description_source = sourceTag.text().replace("来源：", "").trim();
			}
			Element subjectTag = doc.select("#docKeywords").first();
			if (subjectTag != null) {
				subject = subjectTag.text().replace("关键字：", "").replace("，", ";").trim();
			}
			Element contributorTag = doc.select("#docEditor").first();
			if (contributorTag != null) {
				contributor = contributorTag.text().replace("责任编辑：", "").trim();
			}
			Element dateTag = doc.select("#docDeliveddate").first();
			if (dateTag != null) {
				date_created = dateTag.text().replace("-", "");
			}
			Element descriptionTag = doc.select("#docSummary").first();
			if (descriptionTag != null) {
				description = descriptionTag.text().replace("摘要：", "").replace("　　", "").trim();
			}
			for (Element aTag : doc.select("a[class*=location]")) {
				if (aTag.text().equals("首页")) {
					continue;
				}
				provider_subject = provider_subject + aTag.text() + ";";
			}

			if (provider_subject.equals("")) {
				Element tableTag = doc.select("table[cellspacing*=16]").first();
				if (tableTag!=null) {
					Element psubTag = tableTag.select("tr > td").first();					
					provider_subject = psubTag.text().trim().replace("您当前位置：首页 > ", "").replace(" > ", ";");				
				}							
			}

			provider_subject = StringHelper.cleanSemicolon(provider_subject);
			creator = StringHelper.cleanSemicolon(creator);
			creator = creator.replace(";and ", ";");

			XXXXObject xObj = new XXXXObject();

			xObj.data.put("leafid", leafid);
			xObj.data.put("docid", docid);
			xObj.data.put("down_date", down_date);
			xObj.data.put("batch", batch);
			xObj.data.put("title", title);
			xObj.data.put("creator", creator);
			xObj.data.put("description_source", description_source);
			xObj.data.put("date_created", date_created);
			xObj.data.put("subject", subject);
			xObj.data.put("contributor", contributor);
			xObj.data.put("description", description);
			xObj.data.put("provider_subject", provider_subject);

			context.getCounter("map", "count").increment(1);

			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(leafid + "_" + docid), new BytesWritable(bytes));

		}

	}

}