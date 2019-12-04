package simple.jobstream.mapreduce.site.aiaa_meeting;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
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
	//继承Mapper接口,设置map的输入类型为<Object,Text>
	//输出类型为<Text,IntWritable>
	public static class ProcessMapper extends Mapper<LongWritable, Text, Text, BytesWritable> {
		public String batch = "";	
		
		public void setup(Context context) throws IOException,
			InterruptedException {
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
	    	Type type = new TypeToken<Map<String, String>>(){}.getType();
	    	Map<String, String> mapJson = gson.fromJson(value.toString(), type);
	    	
	    	String url = mapJson.get("url").trim();
	    	String session = mapJson.get("session").trim();
	    	String eisbn = mapJson.get("eisbn").trim();
	    	String down_date = mapJson.get("down_date").trim();
	    	String html = mapJson.get("htmlText").trim();
	    	
			Document doc = Jsoup.parse(html);						
            
			LinkedHashMap<String, String> mapcre_ins = new LinkedHashMap<String, String>();
			String baseurl = "https://arc.aiaa.org/doi";
			String title = "";
			String accept_date = "";
			String pub_date = "";
			String author = "";
			String author_1st = "";
			String meeting_name = "";
			String meeting_place = "";
			String subject_word = "";
			String meeting_record_name = "";
			String pub_year = "";
			
			String[] temp;
			String[] temp2;
			String s_split = "•";
			String t_split = ",";
			String delimeter = "-";
			String begintime = "";
			String endtime = "";
			String all_time = "";
			String yue = "";
			String ye = "";
			String on_time ="";
			String on_times = "";
			String publisher ="American Institute of Aeronautics and Astronautics";
			
			String doi = "";
			String lngid = "";
			String rawid = "";
			String product = "AIAA";
			String sub_db = "HY"; 
			String provider = "AIAA";
			String sub_db_id = "00155";
			String source_type = "6";
			String provider_url = url;
			String country = "US";
			String language = "EN";
			
			
			
			//标题
            Element titleTag = doc.select("h1").first();
            if (titleTag !=null) {        
            	title = titleTag.text();
			}
            
            //作者
            Element authorTag = doc.select("ul.rlist--inline.loa.mobile-authors.visible-xs").first();
            if (authorTag !=null) {        
            	author = authorTag.text().replace(" and  ", ";").replace(",  ", ";").replace(" ", "");
            	if(author.contains(";")) {
            		author_1st = author.split(";")[0];
            	}
            	else {
            		author_1st = author;
            	}
			}
            
            //会议时间
            if(!session.equals("")) {
            	temp = session.split(s_split); // 分割字符串
            	if (temp.length > 2) {	                	                	
            		meeting_record_name = temp[0].trim();
                    on_time = temp[1].trim();
                }
                if(!on_time.equals("")) {
	            	 temp2 = on_time.split(t_split);
            		 on_times = temp2[1].trim();
                     accept_date = stdDate(on_times);
                }else {
	               	 Element timesTag = doc.select("div.this-book__meta > div").get(1);
	                 if (timesTag !=null) {
	                 	temp = timesTag.text().split(delimeter); // 分割字符串
	                        begintime = temp[0].trim();
	                        if(begintime.length()>2) {
	                        	try {
	                        		endtime = temp[1].trim();
	                        		temp = endtime.split(" ");
		                            ye = temp[2];
		                            all_time = begintime +" "+ ye;
		                            accept_date = stdDate(all_time);
	                        		
	                        	}catch (Exception e) {
	                        		accept_date = "1900";
	                        	}
	                            
	                            
                        }else {
                        	endtime = temp[1];
                            temp = endtime.split(" ");
                            yue = temp[1].trim();
                            ye = temp[2].trim();
                            all_time = begintime +" "+ yue + " " + ye;
                            accept_date = stdDate(all_time);
                        }
     			}
            }
     			}else {
            	 Element timesTag = doc.select("div.this-book__meta > div").get(1);
                 if (timesTag !=null) {
                 	temp = timesTag.text().split(delimeter); // 分割字符串
                        begintime = stdDate(temp[0]);
                        accept_date = begintime;
     			}
            	
            }


            
            //出版时间
            Element timeTag = doc.select("span.epub-section__date").first();
            if (timeTag !=null) {        
            	pub_date = stdDate(timeTag.text());
            	pub_year = pub_date.substring(0, 4).trim();
			}
            
            //所属会议
            Element setTag = doc.select("div.this-book__meta > div > a").get(0);
            if (setTag !=null) {
            	meeting_name = setTag.text();
			}
            

            
            
            //会议地址
            Element addressTag = doc.select("div.this-book__meta > div").get(2);
            if (addressTag !=null) {
            	meeting_place = addressTag.text();
			}
            
            //topics
            Element topicsTag = doc.select("div.tab__pane--details > ul.rlist > li > a").first();
            if (topicsTag !=null) {
            	subject_word = topicsTag.text();
            }
            
            doi = url.replace("https://arc.aiaa.org/doi/","");
            rawid = doi;
            if (rawid.equals("")) {
            	context.getCounter("map", "error rawid").increment(1);
            	return;
			}
            lngid = VipIdEncode.getLngid(sub_db_id,rawid,false);
            
            
            

			XXXXObject xObj = new XXXXObject();
			
			xObj.data.put("author",author);
			xObj.data.put("author_1st",author_1st);
			xObj.data.put("meeting_record_name",meeting_record_name);
			xObj.data.put("title",title);
			xObj.data.put("doi",doi);
			xObj.data.put("accept_date",accept_date);
			xObj.data.put("pub_date",pub_date);
			xObj.data.put("pub_year",pub_year);
			xObj.data.put("meeting_name",meeting_name);
			xObj.data.put("meeting_place",meeting_place);
			xObj.data.put("subject_word",subject_word);
			xObj.data.put("eisbn",eisbn);
			xObj.data.put("publisher",publisher);
			

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
			xObj.data.put("down_date",down_date);
			xObj.data.put("batch",batch);
			
		
			context.getCounter("map", "count").increment(1);

			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(rawid), new BytesWritable(bytes));
            
	    }

	}
}