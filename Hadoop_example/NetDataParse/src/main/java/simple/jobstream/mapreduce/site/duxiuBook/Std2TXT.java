package simple.jobstream.mapreduce.site.duxiuBook;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import com.almworks.sqlite4java.SQLiteConnection;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

//输入应该为去重后的html
public class Std2TXT extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger.getLogger(Std2TXT.class);

	private static String postfixDb3 = "duxiu_ts";
	private static String tempFileDb3 = "/RawData/chaoxing/duxiu_ts/ref_file/duxiu_ts_template.db3";

	private static boolean testRun = false;
	private static int testReduceNum = 2;
	private static int reduceNum = 1;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";

	public void pre(Job job) {
		String jobName = "StdDuxiu";
		if (testRun) {
			jobName = "test_" + jobName;
		}
		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
		job.setJobName(jobName);
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
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));

		//job.setInputFormatClass(SimpleTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(ProcessReducer.class);

		TextOutputFormat.setCompressOutput(job, false);

		if (testRun) {
			job.setNumReduceTasks(testReduceNum);
		} else {
			job.setNumReduceTasks(reduceNum);
		}
	}

	// ======================================处理逻辑=======================================
	public static class ProcessMapper extends Mapper<Text, BytesWritable, Text, NullWritable> {

		HashSet<String> setRawid = new HashSet<String>();
		HashSet<String> setLngid = new HashSet<String>();

		private static HashMap<String, String> FirstClassMap      = new HashMap<>();
		private static HashMap<String, String> SecondOnlyClassMap = new HashMap<>();
		private static HashMap<String, String> SecondClassMap     = new HashMap<>();		
		private static ClassType classtype = null;
		
		public void setup(Context context) throws IOException,InterruptedException 
		{
			String firstclass_info  = "/RawData/_rel_file/FirstClass.txt";
			String secondclass_info = "/RawData/_rel_file/SecondClass.txt";
			ClassLoad classload = new ClassLoad(context, firstclass_info, secondclass_info);
			
			FirstClassMap      = classload.getfirstclass();
			SecondOnlyClassMap = classload.getsecondonlyclass();
			SecondClassMap     = classload.getsecondclass();
			
			classtype = new ClassType(FirstClassMap, SecondClassMap, SecondOnlyClassMap);
		}

		public void cleanup(Context context) throws IOException, InterruptedException {

		}

		//记录日志到HDFS
		public boolean log2HDFSForMapper(Context context, String text) {
			Date dt = new Date();//如果不需要格式,可直接用dt,dt就是当前系统时间
			DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");//设置显示格式
			String nowTime = df.format(dt);//用DateFormat的format()方法在dt中获取并以yyyy/MM/dd HH:mm:ss格式显示

			df = new SimpleDateFormat("yyyyMMdd");//设置显示格式
			String nowDate = df.format(dt);//用DateFormat的format()方法在dt中获取并以yyyy/MM/dd HH:mm:ss格式显示

			text = nowTime + "\n" + text + "\n\n";

			boolean bException = false;
			BufferedWriter out = null;
			try {
				// 获取HDFS文件系统  
				FileSystem fs = FileSystem.get(context.getConfiguration());

				FSDataOutputStream fout = null;
				String pathfile = "/walker/log/log_map/" + nowDate + ".txt";
				if (fs.exists(new Path(pathfile))) {
					fout = fs.append(new Path(pathfile));
				} else {
					fout = fs.create(new Path(pathfile));
				}

				out = new BufferedWriter(new OutputStreamWriter(fout, "UTF-8"));
				out.write(text);
				out.close();

			} catch (Exception ex) {
				bException = true;
			}

			if (bException) {
				return false;
			} else {
				return true;
			}
		}

		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {

			String title_c = "";//标题
			String Showwriter = "";//作者
			String press_year = "";//发行商，地区，年份
			String tsisbn = "";//isbn号
			String Pagecount = "";//页码数
			String tsseriesname = "";//丛书名
			String tsprice = "";//价格
			String keyword_c = "";//关键字
			String class_ = "";//中图分类
			String remark_c = "";//内容提要
			String Strreftext = "";//参考文献格式
			String rawid = "";//
			String d = "";//
			//
			String NetFullTextAddr = "";
			String NetFullTextAddr_all = "";
			String lngID = "";
			String tspubdate = "";
			String years = "";
			String tsprovinces = "";
			String tspress = "";
			//
			String language = "1";
			String type = "7";
			String titletype = "0;1;1792;1793";
			String srcID = "VIP";

			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("lngid")) {
					lngID = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("isbn")) {
					tsisbn = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("title")) {
					title_c = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("author")) {
					Showwriter = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("page_cnt")) {
					Pagecount = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("title_series")) {
					tsseriesname = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("price")) {
					tsprice = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("keyword")) {
					keyword_c = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("subject")) {
					class_ = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("abstract")) {
					remark_c = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("citation")) {
					Strreftext = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pub_date")) {
					tspubdate = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pub_year")) {
					years = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("provider_url")) {
					NetFullTextAddr = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("publisher")) {
					tspress = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pub_place")) {
					tsprovinces = updateItem.getValue().trim();
				}
				
			}
			
			String oldlngID = String.format("DU_TS_%d", (Long.parseLong(rawid) * 2 + 7));
			
			

			context.getCounter("map", "count").increment(1);
			//String lineOutput = AccessionNumber + "\t" + Authors + "\t" + AuthorAffiliation + "\t" + CorrAuthorAffiliation;
			String line = oldlngID+ "★" + lngID;

			context.write(new Text(line), NullWritable.get());

		}
	}

	public static class ProcessReducer extends Reducer<Text, NullWritable, Text, NullWritable> {


		protected void setup(Context context) throws IOException, InterruptedException {
			
		}

		public void reduce(Text key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {

			context.getCounter("reduce", "count").increment(1);
			context.write(key, NullWritable.get());
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			
		}

	}
}