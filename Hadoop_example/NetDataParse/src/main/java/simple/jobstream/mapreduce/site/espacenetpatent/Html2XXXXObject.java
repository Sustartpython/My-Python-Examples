package simple.jobstream.mapreduce.site.espacenetpatent;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
//import org.apache.tools.ant.taskdefs.Length;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.BXXXXObject;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.SimpleTextInputFormat;
import com.process.frame.util.VipcloudUtil;

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class Html2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 40;
	private static int reduceNum = 40;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	//这个目录会被删除重建
  
	public void pre(Job job)
	{
		String jobName = "espacenetpatent." + this.getClass().getSimpleName();

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

	public void SetMRInfo(Job job)
	{
		job.getConfiguration().set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println(job.getConfiguration().get("io.compression.codecs"));

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(ProcessReducer.class);
		
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(BytesWritable.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(BytesWritable.class);
	    
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);

	    
		SequenceFileOutputFormat.setCompressOutput(job, false);
		if (testRun) {
			job.setNumReduceTasks(testReduceNum);
		} else {
			job.setNumReduceTasks(reduceNum);
		}
	}

	public void post(Job job)
	{
	
	}

	public String GetHdfsInputPath()
	{
		return inputHdfsPath;
	}

	public String GetHdfsOutputPath()
	{
		return outputHdfsPath;
	}

	
	public static class ProcessMapper extends 
			Mapper<LongWritable, Text, Text, BytesWritable> {
		//记录日志到HDFS
		public boolean log2HDFSForMapper(Context context, String text) {
			Date dt=new Date();//如果不需要格式,可直接用dt,dt就是当前系统时间
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
		        String pathfile = "/RawData/epregister/log/" + nowDate + ".txt";
		        // 判断是否存在日志目录
		        if (fs.exists(new Path(pathfile))) {
		        	fout = fs.append(new Path(pathfile));
				}
		        else {
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
			}
			else {
				return true;
			}
		}
		static int cnt = 0;	
		public static HashMap<String,String> map = new HashMap<String,String>();
		
		
		public static HashMap<String,String> parseHtml(Context context, String htmlText){
			//测试数据
			map.put("raw_id", "EP3493525NWA2");
			map.put("pub_date","20190605");
			map.put("url","https://data.epo.org/publication-server/html-document?PN=EP3493525%20EP%203493525&iDocId=5945753");
			
			//初始化数据
			String rawid = "";
			String url = "";
			String pub_date = "";
			String title = "";
			String identifier_standard = "";
			String creator= "";
			String title_alternative= "";
			String applicant= "";
			String creator_institution= "";
			String date= "";
			String description= "";
			String date_impl= "";
			String date_created= "";
			String priority_number= "";
			String language= "";
			String country= "EP";
			String identifier_pissn= "";
			
			//解析数据
		    rawid = map.get("raw_id");
			url=map.get("url");
			pub_date=map.get("pub_date");
			if (pub_date!="") {
				date=pub_date.substring(0, 4);
			}
			date_created=pub_date;
			Document doc = Jsoup.parse(htmlText);
			Elements titleElements=doc.select("td:containsOwn((54))");
			if (titleElements!=null) {
				Element titleElement=titleElements.first().nextElementSibling();
				if (titleElement!=null) {
					if (titleElement.attr("class").equals("t1 bold")) {
						title=titleElement.text().trim();
					} else {
						Elements titleElements2=titleElement.getElementsByTag("p");
						if (titleElements2!=null) {
							title=titleElements2.first().text().trim();
							title_alternative=titleElements2.first().nextElementSibling().text().trim();
						}
					}
				}
			}
			
			Element identifier_standardElement=doc.select("td:containsOwn((11))").first().nextElementSibling();
			if (identifier_standardElement!=null) {
				identifier_standard=identifier_standardElement.text().replaceAll("[\\s\\u00A0]", "").trim();
			}
			Element creatorElement=doc.select("td:containsOwn((72))").first().parent().nextElementSibling();
			if (creatorElement!=null) {
				creator=creatorElement.getElementsByTag("td").html().replaceAll("[\\n]", "").replaceAll("<ul> <li>", "").replaceAll("<br>", ";[").replaceAll("\\[.*?<li>", "").replaceAll("\\[.*?</ul>", "").replaceAll(";$", "").trim();
			}
			Element applicantElement=doc.select("td:containsOwn((71))").first().nextElementSibling();
			if (applicantElement!=null) {
				applicant=applicantElement.getElementsByTag("span").text().trim();
			}
			Element creator_institutionElement=doc.select("td:containsOwn((71))").first().parent().nextElementSibling();
			if (creator_institutionElement!=null) {
				creator_institution=creator_institutionElement.getElementsByTag("td").html().replaceAll("[\\n]", "").replaceAll("<ul> <li>", "").replaceAll("<br>", ";[").replaceAll("\\[.*?<li>", "").replaceAll("\\[.*?</ul>", "").replaceAll(";$", "").replaceAll("&nbsp;", "").trim();
			}
			Element descriptionElement=doc.select("div[data-part=\"description\"]").first();
			if (descriptionElement!=null) {
				description=descriptionElement.text().replaceAll(".*?\\[0001\\]", "[0001]").trim();
			}
			Element date_implElement=doc.select("td:containsOwn((22))").first().nextElementSibling().select("span[class=\"skiptranslate\"]").first();
			if (date_implElement!=null) {
				date_impl=date_implElement.text().trim().split("\\.")[2]+date_implElement.text().trim().split("\\.")[1]+date_implElement.text().trim().split("\\.")[0];
			}
			Elements priority_numberElements=doc.select("td:containsOwn((30))");
			if (priority_numberElements!=null) {
				Element priority_numberElement2=priority_numberElements.first();
				if (priority_numberElement2!=null) {
					Element priority_numberElement3=priority_numberElement2.nextElementSibling();
					if (priority_numberElement3!=null) {
						Element priority_numberElement4=priority_numberElement3.nextElementSibling();
						if (priority_numberElement4!=null) {
							priority_number=priority_numberElement4.text().trim();
						}
					}
				}
				
			}
			Elements languageElements=doc.select("div[data-part=\"description\"]");
			if (languageElements!=null) {
				Element languageElement=languageElements.first().getElementsByTag("div").first();
				if (languageElement!=null) {
					language=languageElement.attr("lang").toUpperCase();
				}
			}
			Elements identifier_pissnElements=doc.select("td:containsOwn((21))");
			if (identifier_pissnElements!=null) {
				Element identifier_pissnElement=identifier_pissnElements.first().nextElementSibling().getElementsByTag("span").first();
				if (identifier_pissnElement!=null) {
					identifier_pissn=identifier_pissnElement.text();
				}
			}
			
			
			
			System.out.println("language:"+language);
//			System.out.println("applicant:"+applicant);
			
			
			
			//储存数据
			map.put("rawid", rawid);
			map.put("pub_date", pub_date);
			map.put("url", url);
			map.put("title", title);
			map.put("identifier_standard", identifier_standard);
			map.put("creator",creator);
			map.put("title_alternative",title_alternative);
			map.put("applicant",applicant);
			map.put("creator_institution",creator_institution);
			map.put("date",date);
			map.put("description",description);
			map.put("date_impl",date_impl);
			map.put("date_created",date_created);
			map.put("priority_number",priority_number);
			map.put("language",language);
			map.put("country",country);
			map.put("identifier_pissn",identifier_pissn);

			
			return map;

		}
		
	    public void map(LongWritable key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	    	cnt += 1;
	    	if (cnt == 1) {
				System.out.println("text:" + value.toString());
			}
	    	String text = value.toString().trim();
	    	Gson gson = new Gson();
			Type type = new TypeToken<Map<String, Object>>() {}.getType();
			Map<String, String> mapField = gson.fromJson(text, type);
			map.put("raw_id", mapField.get("id").toString().trim());
			map.put("pub_date",mapField.get("pub_date").toString().trim());
			map.put("url",mapField.get("url").toString().trim());
			
			HashMap<String,String> myMap = parseHtml(context, mapField.get("html").toString().trim());
	    	
	    	if (myMap == null){
	    		log2HDFSForMapper(context, "##" + value.toString() + "**");
	    		context.getCounter("map", "null").increment(1);
	    		return;
	    	}
	    	
	    	
			XXXXObject xObj = new XXXXObject();
			xObj.data.put("rawid", myMap.get("rawid"));
			xObj.data.put("pub_date", myMap.get("pub_date"));
			xObj.data.put("url", myMap.get("url"));
			xObj.data.put("title", myMap.get("title"));
			xObj.data.put("identifier_standard", myMap.get("identifier_standard"));
			xObj.data.put("creator",myMap.get("creator"));
			xObj.data.put("title_alternative",myMap.get("title_alternative"));
			xObj.data.put("applicant",myMap.get("applicant"));
			xObj.data.put("creator_institution",myMap.get("creator_institution"));
			xObj.data.put("date",myMap.get("date"));
			xObj.data.put("description",myMap.get("description"));
			xObj.data.put("date_impl",myMap.get("date_impl"));
			xObj.data.put("date_created",myMap.get("date_created"));
			xObj.data.put("priority_number",myMap.get("priority_number"));
			xObj.data.put("language",myMap.get("language"));
			xObj.data.put("country",myMap.get("country"));
			xObj.data.put("identifier_pissn",myMap.get("identifier_pissn"));
			byte[] bytes = com.process.frame.util.VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(myMap.get("db") + "_" + myMap.get("rawid")  ), new BytesWritable(bytes));			
		}    
	}
	

	public static class ProcessReducer extends
		Reducer<Text, BytesWritable, Text, BytesWritable> {
		public void reduce(Text key, Iterable<BytesWritable> values, 
		                   Context context
		                   ) throws IOException, InterruptedException {
			
			
			BytesWritable bOut = new BytesWritable();	//用于最后输出
			for (BytesWritable item : values) {
				if (item.getLength() > bOut.getLength()) {	//选最大的一个
					bOut = item;
				}
			}			
			context.getCounter("reduce", "count").increment(1);
		
			context.write(key, bOut);
		}
	}
}
