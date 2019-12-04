package simple.jobstream.mapreduce.site.pubmed;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.status_jsp;
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
//import org.apache.tools.ant.taskdefs.Length;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.BXXXXObject;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.SimpleTextInputFormat;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.AuthorOrgan;
import simple.jobstream.mapreduce.common.vip.LogMR;
import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;
import simple.jobstream.mapreduce.site.pubmed.GetData;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.status_jsp;
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
//import org.apache.tools.ant.taskdefs.Length;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.BXXXXObject;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.SimpleTextInputFormat;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.AuthorOrgan;
import simple.jobstream.mapreduce.common.vip.LogMR;
import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;
import simple.jobstream.mapreduce.site.pubmed.GetData;


public class parseCountry extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 1;

	private static int reduceNum = 1;
	
	public static  String inputHdfsPath = "";
	public static  String outputHdfsPath = "";
	public static String [] coveridArray = null;

	
	public void pre(Job job) {
		String jobName = "parsepubmed";
		if (testRun) {
			jobName = "test_" + jobName;
		}
		
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
		//job.setOutputFormatClass(TextOutputFormat.class);
		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.9f);
		System.out.println("******mapred.reduce.slowstart.completed.maps*******"
				+ job.getConfiguration().get("mapred.reduce.slowstart.completed.maps"));
		job.getConfiguration().set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println(job.getConfiguration().get("io.compression.codecs"));


		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(ProcessReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setCompressOutput(job, false);

		if (testRun) {
			job.setNumReduceTasks(testReduceNum);
		} else {
			job.setNumReduceTasks(reduceNum);
		}
	}


	public static class ProcessMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		int cnt = 0;

		private static String country = "US";
		private static String language = "EN";
		private static String country_language = "";
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			{
				country = "US";
				language = "EN";		
				country_language="";

			}

			String logHDFSFile = "/user/qianjun/log/log_map/pubmed/" + DateTimeHelper.getNowDate() + ".txt";

			cnt += 1;
			if (cnt == 1) {
				System.out.println("text:" + value.toString());
			}

			String text = value.toString().trim();

			Gson gson = new Gson();
			Type type = new TypeToken<Map<String, JsonElement>>() {
			}.getType();
			Map<String, JsonElement> mapField = gson.fromJson(text, type);

			// 获取journal相关信息
			if (mapField.get("MedlineCitation").getAsJsonObject().has("Article")) {
				// 获取文章相关信息对象article
				JsonObject articleObjt = mapField.get("MedlineCitation").getAsJsonObject().get("Article")
						.getAsJsonObject();			
				language= GetData.getData(articleObjt, "Language", "", "");	
				
				
			}
	
			// 获取国家相关信息，
			if (mapField.get("MedlineCitation").getAsJsonObject().has("MedlineJournalInfo")) {
				// 获取文章相关信息对象article
				JsonObject medlineObjt = mapField.get("MedlineCitation").getAsJsonObject().get("MedlineJournalInfo")
						.getAsJsonObject();
				country= GetData.getData(medlineObjt, "Country", "", "");
			}
			if (country.length() <1) {
				country = "US";
				
			}
			if(language.length() <1) {
				language = "EN";
				
			}
			country_language = country +"_" +language;
			context.getCounter("map", "total").increment(1);
			context.write(new Text(language), NullWritable.get());

		}
	}
	public static class ProcessReducer extends
	Reducer<Text, NullWritable, Text, NullWritable> {

		public void reduce(Text key, Iterable<NullWritable> values,
		Context context) throws IOException, InterruptedException {
	
	context.getCounter("reducer", "count").increment(1);
	context.write(key, NullWritable.get());
}

}
}
