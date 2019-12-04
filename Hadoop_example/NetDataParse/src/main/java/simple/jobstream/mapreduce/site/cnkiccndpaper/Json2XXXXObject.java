package simple.jobstream.mapreduce.site.cnkiccndpaper;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.BXXXXObject;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.SimpleTextInputFormat;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.LogMR;
import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;
import simple.jobstream.mapreduce.site.naturejournal.nature_journal_db;

//import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class Json2XXXXObject extends InHdfsOutHdfsJobInfo {

	private static int reduceNum = 0;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = ""; // 这个目录会被删除重建

	public void pre(Job job) {
		job.setJobName(job.getConfiguration().get("jobName"));
		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
		reduceNum = Integer.parseInt(job.getConfiguration().get("reduceNum"));
	}

	public String getHdfsInput() {
		return inputHdfsPath;
	}

	public String getHdfsOutput() {
		return outputHdfsPath;
	}

	public void SetMRInfo(Job job) {

		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.7f);
		System.out.println("******mapred.reduce.slowstart.completed.maps*******"
				+ job.getConfiguration().get("mapred.reduce.slowstart.completed.maps"));
		job.getConfiguration().set("io.compression.codecs",
				"org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(UniqXXXXObjectReducer.class);
//		job.setReducerClass(ProcessReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

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

	public static class ProcessMapper extends Mapper<LongWritable, Text, Text, BytesWritable> {
		private static String logHDFSFile = "/user/qianjun/log/log_map/" + DateTimeHelper.getNowDate() + ".txt";

		static int cnt = 0;
		private static String rawid = "";
		private static String db = "";
		private static String km = "";
		private static String title = "";
		private static String otherTitle = "";
		private static String description = "";
		private static String date_created = "";
		private static String subject_clc = "";
		private static String identifier_cnno = "";
		private static String title_series = "";
		private static String title_edition = "";
		private static String title_catalyst = "";
		private static String down_cnt = "";
		private static String page = "";
		private static String description_type = "";
		private static String creator = "";
		private static String batch = "";
		private static String down_date= "";
		private static String creator_release="";
		private static String province_code="";
		private static String pub_place="";
		private static String description_cycle="";
		private static String sub_db_id="00080";
		private static String provider="";
		private static String provider_id="";
		private static String provider_url="";
		private static String gch="";
		private static String source="";
		private static String date="";
		private static String publisher="";
		private static String type="11";
		private static String medium="2";
		private static String lngid="";
		private static String cover="";
		private static String coverstat="";
		private static String new_down_cnt="";
		private static HashMap<String, HashMap<String, String>> journalInfo = new HashMap<String, HashMap<String, String>>();
	
		
		public void setup(Context context) throws IOException, InterruptedException {
			batch = context.getConfiguration().get("batch");
			context.getCounter("batch", batch).increment(1);
			initJournalInfo(context);
		}
		private static void initJournalInfo(Context context) throws IOException {
			// 获取HDFS文件系统
			FileSystem fs = FileSystem.get(context.getConfiguration());

			FSDataInputStream fin = fs.open(new Path("/RawData/cnki/cnkiccndpaper/newsinfo/cnki_newspaper_journalinfo.txt"));
			BufferedReader in = null;
			String line;
			try {
				in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
				while ((line = in.readLine()) != null) {
					line = line.trim();
					if (line.length() < 1) {
						continue;
					}
					Gson gson = new Gson();
					Type type = new TypeToken<HashMap<String, String>>() {
					}.getType();
					HashMap<String, String> mapField = gson.fromJson(line, type);
					journalInfo.put(mapField.get("pykm"), mapField);
				}
			} finally {
				if (in != null) {
					in.close();
				}
			}
			System.out.println("journalInfo size: " + journalInfo.size());
		}

		// String doi, String jid,
		public void parseHtml(String htmlText) {

			Document doc = Jsoup.parse(htmlText.toString());
			title="";

			// 获取标题
			Element TitleElement = doc.select("div[class=wxTitle]").first();
			
			if (TitleElement != null) {

				Element titleElement = TitleElement.select("h2[class=title]").first();
				title = titleElement.text().trim();

			}
			// 获取作者
			Element authorElement = doc.select("div[class=author]").first();
			if(authorElement!=null) {			
			Elements allauthor = authorElement.select("span");
			for (Element data : allauthor) {
				String author = data.text();
				creator = creator + author + ";";
			}
			}
         // 正文信息
			Element articleElement = doc.select("div[class=wxBaseinfo]").first();
			if(articleElement != null) {
			Elements allp = articleElement.select("p");
			for (Element data : allp) {
//                    System.out.println(data.text());
				if (data.text().startsWith("副标题")) {
					otherTitle = data.text().replace("副标题：", "");
				} else if (data.text().startsWith("正文快照")) {
					description = data.text().replace("正文快照：", "");
				} else if (data.text().startsWith("报纸日期")) {
					date_created = data.text().replace("报纸日期：", "").replace("-", "").replace("/", "").replace("-", "").replace(" ", "").trim();
			        String regEx="[^0-9]";
			        Pattern p = Pattern.compile(regEx);
			        Matcher m = p.matcher(date_created);
			    
			        date_created = m.replaceAll("").trim();
			  
				} else if (data.text().startsWith("分类号")) {
					subject_clc = data.text().replace("分类号：", "").replace("-", "");
				} else if (data.text().startsWith("版名")) {
					title_series = data.text().replace("版名：", "").replace("-", "");
				} else if (data.text().startsWith("版号")) {
					title_edition = data.text().replace("版号：", "").replace("-", "");
				} else if (data.text().startsWith("版号")) {
					title_edition = data.text().replace("版号：", "").replace("-", "");
				} else if (data.text().startsWith("引题")) {
					title_catalyst = data.text().replace("引题：", "").replace("-", "");
				}

			}}
				// 获取下载量，以及页数
				Element pageinfoElement = doc.select("div[class=total]").first();
				if(pageinfoElement!=null) {
				Elements allspan = pageinfoElement.select("span");
				for (Element data : allspan) {
	
					if (data.text().startsWith("下载：")) {
						down_cnt = data.text().replace("下载：", "");
					} else if (data.text().startsWith("页数：")) {
						page = data.text().replace("页数：", "");
					}
				}}
				

		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			{	lngid="";
				rawid = "";
				db = "";
				km = "";
				title = "";
				otherTitle = "";
				description = "";
				date_created = "";
				subject_clc = "";
				identifier_cnno = "";
				title_series = "";
				title_edition = "";
				title_catalyst = "";
				down_cnt = "";
				page = "";
				description_type = "";
				creator = "";
				batch = "";
				down_date= "";
				creator_release="";
				province_code="";
				pub_place="";
				description_cycle="";
				sub_db_id="00080";
				provider="";
				provider_id="";
				provider_url="";
				gch="";
				source="";
				date="";
				publisher="";
				type="11";
				medium="2";
				down_date="20190430";
				cover="";
				coverstat="";
				new_down_cnt="";
				
			}

			cnt += 1;
			if (cnt == 1) {
				System.out.println("text:" + value.toString());
			}

			String text = value.toString().trim();

			Gson gson = new Gson();
			Type type = new TypeToken<Map<String, Object>>() {
			}.getType();

			Map<String, Object> mapField = gson.fromJson(text, type);
			if (mapField ==null) {
				return;
			}
			if (mapField.containsKey("doi")) {
				rawid= mapField.get("doi").toString();
			}
            db = mapField.get("dbCode").toString();
            km= mapField.get("pykm").toString();
         
			String htmlText = mapField.get("html").toString();
			if (mapField.containsKey("down_date")) {
				down_date = mapField.get("down_date").toString();
			}

			if (!htmlText.toLowerCase().contains("wxmain")) {
				context.getCounter("map", "Error: wxmain").increment(1);
				return;
			}
			if(rawid.length() <1) {
				context.getCounter("map", "Error: no rawid").increment(1);
				return;
			}
			if (!htmlText.contains(rawid)) {
				context.getCounter("map", "Error: not find rawid").increment(1);
				return;
			}
		
			parseHtml(htmlText);

			if (title.length() < 1) {
				context.getCounter("map", "Error: no title").increment(1);
				LogMR.log2HDFS4Mapper(context, logHDFSFile, "error: no title: " + rawid);
				return;
			}
			// 处理作者最后面的分号
			creator = creator.replaceAll(";$", "");
			// 处理日期
			if(date_created.length() ==8) {
				date = date_created.substring(0, 4);	
				context.getCounter("map", "date size 8").increment(1);
			}else {
				LogMR.log2HDFS4Mapper(context, logHDFSFile, "error: date_created: " + date_created);
				date ="1900";
				date_created="19000000";
			}
	
			provider = "cnkiccndpaper";
			provider_id = provider +"@" + rawid;
			provider_url = provider +"@"+"http://kns.cnki.net/kcms/detail/detail.aspx?dbcode="+db+"&filename=" + rawid;
			gch = provider + "@" +km;

			lngid = VipIdEncode.getLngid(sub_db_id, rawid, false);
		
			// 获取报纸信息
			if(journalInfo.containsKey(km)) {
				if(journalInfo.get(km).containsKey("报纸名称")) {
					source = journalInfo.get(km).get("报纸名称");
					
				}
				if(journalInfo.get(km).containsKey("主办单位")) {
					creator_release =  journalInfo.get(km).get("主办单位");
				}
				if(journalInfo.get(km).containsKey("出版地")) {
					pub_place =  journalInfo.get(km).get("出版地");	
				}
				if(journalInfo.get(km).containsKey("国内统一刊号")) {
					identifier_cnno =  journalInfo.get(km).get("国内统一刊号").replace("CN", "").trim();		
				}
				if(journalInfo.get(km).containsKey("刊期")) {
					description_cycle =  journalInfo.get(km).get("刊期");			
				}
				if(journalInfo.get(km).containsKey("邮编")) {
					province_code =  journalInfo.get(km).get("邮编");		
				}
				if(journalInfo.get(km).containsKey("级别")) {
					description_type=  journalInfo.get(km).get("级别");		
				}
				if(journalInfo.get(km).containsKey("级别")) {
					description_type=  journalInfo.get(km).get("级别");	
				}
				if(journalInfo.get(km).containsKey("coverstat")) {
					coverstat=  journalInfo.get(km).get("coverstat");	
				}

			}
			if(coverstat.equals("1")) {
				cover = "/smartlib/cnkiccndpaper/" +km.toLowerCase()+ ".jpg";
			}else {
				context.getCounter("map", "Error: no cover").increment(1);
			}
			batch = (new SimpleDateFormat("yyyyMMdd")).format(new Date()) + "00";
			// 处理下载次数
			if(down_cnt.length() >0) {
				new_down_cnt=down_cnt +"@" +"20190612";
			}
			
			XXXXObject xObj = new XXXXObject();
			xObj.data.put("rawid",rawid);
			xObj.data.put("db",db);
			xObj.data.put("km",km);
			xObj.data.put("title",title);
			xObj.data.put("otherTitle",otherTitle);
			xObj.data.put("description",description);
			xObj.data.put("date_created",date_created);
			xObj.data.put("subject_clc",subject_clc);
			xObj.data.put("identifier_cnno",identifier_cnno);
			xObj.data.put("title_series",title_series);
			xObj.data.put("title_edition",title_edition);
			xObj.data.put("title_catalyst",title_catalyst);
			xObj.data.put("down_cnt",new_down_cnt);
			xObj.data.put("page",page);
			xObj.data.put("description_type",description_type);
			xObj.data.put("creator",creator);
			xObj.data.put("batch",batch);
			xObj.data.put("down_date",down_date);
			xObj.data.put("creator_release",creator_release);
			xObj.data.put("province_code",province_code);
			xObj.data.put("pub_place",pub_place);
			xObj.data.put("description_cycle",description_cycle);
			xObj.data.put("sub_db_id",sub_db_id);
			xObj.data.put("provider",provider);
			xObj.data.put("provider_id",provider_id);
			xObj.data.put("provider_url",provider_url);
			xObj.data.put("gch",gch);
			xObj.data.put("source",source);
			xObj.data.put("date",date);
			xObj.data.put("publisher",creator_release);
			xObj.data.put("lngid",lngid);
			xObj.data.put("type","11");
			xObj.data.put("medium",medium);
			xObj.data.put("cover",cover);

			byte[] bytes = com.process.frame.util.VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(rawid), new BytesWritable(bytes));
			context.getCounter("map", "count").increment(1);

		}
	}
}
