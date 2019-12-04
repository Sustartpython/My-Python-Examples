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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import com.almworks.sqlite4java.SQLiteConnection;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.vip.SqliteReducer;

//输入应该为去重后的html
public class StdDuxiu extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger.getLogger(StdDuxiu.class);
	
	private static int reduceNum = 1;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";

	public void pre(Job job) {
		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
		reduceNum = Integer.parseInt(job.getConfiguration().get("reduceNum"));
		job.setJobName(job.getConfiguration().get("jobName"));
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
		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.7f);
		
		//job.setInputFormatClass(SimpleTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setMapperClass(ProcessMapper.class);
//		job.setReducerClass(ProcessReducer.class);
		job.setReducerClass(SqliteReducer.class);

		TextOutputFormat.setCompressOutput(job, false);

		job.setNumReduceTasks(reduceNum);
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
			
			d = NetFullTextAddr.split("=")[2];

			NetFullTextAddr_all = String.format("{\"CHAOXING@DUXIU\":[\"%s\"]}", NetFullTextAddr);
			
			keyword_c = keyword_c.replace('\0', ' ').replace("'", "''").trim();
			title_c = title_c.replace('\0', ' ').replace("'", "''").trim();
			Showwriter = Showwriter.replace('\0', ' ').replace("'", "''").trim();
			tsisbn = tsisbn.replace('\0', ' ').replace("'", "''").trim();
			Pagecount = Pagecount.replace('\0', ' ').replace("'", "''").trim();
			tsseriesname = tsseriesname.replace('\0', ' ').replace("'", "''").trim();
			tsprice = tsprice.replace('\0', ' ').replace("'", "''").trim();
			class_ = class_.replace('\0', ' ').replace("'", "''").trim();
			remark_c = remark_c.replace('\0', ' ').replace("'", "''").trim();
			Strreftext = Strreftext.replace('\0', ' ').replace("'", "''").trim();
			tspubdate = tspubdate.replace('\0', ' ').replace("'", "''").trim();
			years = years.replace('\0', ' ').replace("'", "''").trim();
			tsprovinces = tsprovinces.replace('\0', ' ').replace("'", "''").trim();
			tspress = tspress.replace('\0', ' ').replace("'", "''").trim();

			String classtypes     = classtype.GetClassTypes(class_);
			String showclasstypes = classtype.GetShowClassTypes(class_);
			String sql = "insert into modify_title_info (lngid,title_c,dx32,showwriter,years,tspress,tsprovinces,tspubdate,class,keyword_c,remark_c,tsisbn,pagecount,tsseriesname,tsprice,strreftext,netfulltextaddr,netfulltextaddr_all,language,type,titletype,rawid,srcid,classtypes,showclasstypes) ";
			sql += " VALUES ('%s', '%s', '%s', '%s','%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s','%s','%s');";
			sql = String.format(sql, lngID, title_c, d, Showwriter, years, tspress, tsprovinces, tspubdate, class_,
					keyword_c, remark_c, tsisbn, Pagecount, tsseriesname, tsprice, Strreftext, NetFullTextAddr,
					NetFullTextAddr_all, language, type, titletype, rawid, srcID,classtypes,showclasstypes);

			context.getCounter("map", "count").increment(1);
			context.write(new Text(sql), NullWritable.get());

		}
	}
}