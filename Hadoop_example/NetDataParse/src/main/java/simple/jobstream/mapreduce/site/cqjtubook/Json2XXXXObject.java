package simple.jobstream.mapreduce.site.cqjtubook;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.time.Year;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.diskbalancer.planner.Step;
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
import org.hamcrest.core.Is;
//import org.apache.tools.ant.taskdefs.Length;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.junit.Test;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.BXXXXObject;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.JobConfUtil;
import com.process.frame.util.SimpleTextInputFormat;
import com.process.frame.util.VipcloudUtil;
import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.LogMR;
import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;
import java.util.*;
import com.google.common.base.Joiner;

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
//		job.getConfiguration().set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
//		System.out.println(job.getConfiguration().get("io.compression.codecs"));
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
		private static String rawid ="";
		private static String lngid ="";
		private static String title ="";
		private static String subject_clc ="";;
		private static String creator ="";
		private static String date_created = "";
		private static String publisher ="";
		private static String provider_subject="";
		private static String medium = "2";
		private static String type1 = "1";
		private static String country = "CN";
		private static String provider ="cqjtubook";
		private static String provider_url ="";
		private static String provider_id ="";
		private static String gch ="";
		private static String subject="";
		private static String batch="";
		private static String date="";
		private static String sub_db_id="";
		private static String language="";

		static String dealsubject(String args){
			String str="";
		    List list=new ArrayList();
		    Set set=new HashSet();
		    List newList = new  ArrayList();
		    String[] news = args.split(" ");//用split()函数直接分割
			for (String string : news) {
			   list.add(string);
			}
			for (Iterator it=list.iterator();it.hasNext();) {
			    //set能添加进去就代表不是重复的元素
			    Object element=it.next();
			    if(set.add(element)){
			        newList.add(element);
			    }
			}

		    str = Joiner.on(",").join(newList);
		    return str.replaceAll(",",";");
}


		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			cnt += 1;
			if (cnt == 1) {
				System.out.println("text:" + value.toString());
			}

			String text = value.toString().trim();

			Gson gson = new Gson();
			Type type = new TypeToken<Map<String, Object>>() {
			}.getType();

			Map<String, Object> mapField = gson.fromJson(text, type);
			provider_subject="";
            provider_subject= mapField.get("classifyname").toString();
            String htmlText = mapField.get("html").toString();
	
			if (!htmlText.toLowerCase().contains("recom_list")) {
				context.getCounter("map", "Error: recom_list").increment(1);
				return;
			}

			if (htmlText.contains("recom_list")) {

                Document doc = Jsoup.parse(htmlText);
                
                Elements booksInfoElement = doc.select("div[class='remtext']");
                for (Element bookInfoElement:booksInfoElement) {
                	
                if (bookInfoElement != null){
                	// 初始化变量              	
                	{
    					rawid ="";
    					lngid ="";
    					title ="";
    					subject_clc ="";;
    					creator ="";
    					date_created = "";
    					publisher ="";
    					medium = "2";
    					type1 = "1";
    					country = "CN";
    					provider ="cqjtubook";
    					provider_url ="";
    					provider_id ="";
    					gch ="";
    					subject="";
    					batch="";
    					sub_db_id ="00039";
    					language = "ZH";
    				}

                    Element HrefElement = bookInfoElement.select("a").first();
                    if (bookInfoElement != null){
                        rawid =HrefElement.attr("href").replace("/front/reader/goRead?ssno=","").replace("&channel=100&jpgread=1","").trim(); 
                       title =HrefElement.attr("title").trim();  
                    }

                    // 获取其他信息节点
                    Elements infosElement = bookInfoElement.select("dd");
                    for (Element oneInfo:infosElement){

                        // 循环拿出节点字符串
                        String rawStr =  oneInfo.text().trim();
                        if( rawStr.startsWith("作者")){
                        	creator  = rawStr.replace("作者","").replace("：","").replace("作者：","").trim().replace(",",";").replaceAll("；",";").replaceAll("，",";").replaceAll("; ", ";").replaceAll(", ",";").replaceAll(",","");

                        }
                        if( rawStr.startsWith("出版日期")){
                        	date= rawStr.replace("出版日期","").replace("：","").replace("出版日期：","").trim();

                        }
                        if( rawStr.startsWith("主题词")){
                            subject = rawStr.replace("主题词","").replace("：","").replace("主题词：","").trim().replace(",",";");
                            if(subject.length() >1 && subject.contains(" ")) {
                            	subject =dealsubject(subject);
                            }
                        }
                        
                        // 处理subject
                        
                        if( rawStr.startsWith("索书号")){
                        	subject_clc = rawStr.replace("索书号","").replace("：","").replace("索书号：","").trim().replace(",",";");
                        }
                    }
                    //处理date_created 
                    if (date.length() <1) {
                    	date ="1900";
                    }
                    if(date.length() ==4) {
                    	date_created = date+"0000";
                    }
                    else if (date.length() >4) {
                    	date_created = date.replace(".", "")+"00";
                    	date = date.substring(0,4);
 			
					}
                    
                    // 生成batch
            		batch = (new SimpleDateFormat("yyyyMMdd")).format(new Date()) + "00";
            		
            		// 生成lngid
            		if (rawid.length() > 0) {
        				lngid = VipIdEncode.getLngid(sub_db_id, rawid, false);
        			}
            		// 处理subject 
            		subject = subject.replaceAll(";;", ";").replaceAll(";$", "");
            		// 生成gch
//            		gch = provider +"@" +rawid;
            		
            		//生成provider_url
            		provider_url =provider +"@" +"http://202.202.244.9:9080"+"/front/reader/goRead?ssno="+rawid+"&channel=100&jpgread=1";
            		
            		//provider_id
            		provider_id = provider +"@" +rawid;
                    
                    // 直接将数据put进xxx里面
                    XXXXObject xObj = new XXXXObject();
        			{
        				xObj.data.put("lngid", lngid);
        				xObj.data.put("rawid", rawid);
        				xObj.data.put("title", title);
        				xObj.data.put("creator", creator);
        				xObj.data.put("provider", provider);
        				xObj.data.put("date_created", date_created);
        				xObj.data.put("batch", batch);
        				xObj.data.put("subject", subject);
//        				xObj.data.put("gch", gch);
        				xObj.data.put("provider_url", provider_url);
        				xObj.data.put("provider_id", provider_id);
        				xObj.data.put("publisher", publisher);
        				xObj.data.put("country", country);
        				xObj.data.put("type", type1);
        				xObj.data.put("medium",medium);
        				xObj.data.put("provider_subject", provider_subject);
        				xObj.data.put("date", date);
        				xObj.data.put("language", language);
        		
        			}

        			byte[] bytes = com.process.frame.util.VipcloudUtil.SerializeObject(xObj);
        			context.write(new Text(rawid), new BytesWritable(bytes));
        			context.getCounter("map", "count").increment(1);
                    
                }
            }
			}
		}


		
	}
}
