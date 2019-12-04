package simple.jobstream.mapreduce.site.asceProceedings;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.print.Doc;

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

import com.cloudera.org.codehaus.jackson.annotate.JsonTypeInfo.None;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;
import simple.jobstream.mapreduce.common.vip.AuthorOrgan;
import simple.jobstream.mapreduce.common.vip.SqliteReducer;
import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;
import simple.jobstream.mapreduce.site.academicjournal.Temp2Latest;
import simple.jobstream.mapreduce.site.cssci.Json2XXXXObject.ProcessMapper;
import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import org.apache.log4j.Logger;

//统计wos和ei的数据量
public class Json2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 50;
	public static Logger logger = Logger.getLogger(Json2XXXXObject.class);

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
		
		private static Map<String, String> utmap = new HashMap<String, String>();
		
//		private void initArrayList(Context context) throws IOException {
//			FileSystem fs = FileSystem.get(context.getConfiguration());
//
//			FSDataInputStream fin = fs.open(new Path("/user/suh/ascelibrary/meeting_intro.txt"));
//
//			BufferedReader reader = null;
//			try {
//				reader = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
//				String temp;
//
//				while ((temp = reader.readLine()) != null) {
//					temp = temp.trim();
//					if(temp.split("\t").length == 2) {
//						utmap.put(temp.split("\t")[0],temp.split("\t")[1]);
//					}					
//				}
//
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//			} finally {
//				if (reader != null) {
//					reader.close();
//				}
//			}
//
//		}
		
		
		public void setup(Context context) throws IOException,
			InterruptedException {
			batch = context.getConfiguration().get("batch");
//			initArrayList(context);
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
	    	String isbn = mapJson.get("isbn").trim();
	    	String eisbn = mapJson.get("eisbn").trim();
	    	String subject_word = mapJson.get("keyword").trim();
	    	String meeting_record_name = mapJson.get("meeting_name").trim();
	    	String meeting_place = mapJson.get("meeting_place").trim();
	    	String before_time = mapJson.get("accept_date").trim();
	    	String page_info = mapJson.get("pages").replace("pp.", "").trim();
	    	String down_date = mapJson.get("down_date").trim();
	    	String meeting_intro = mapJson.get("meeting_intro").trim();
	    	String html = mapJson.get("htmlText").trim();
			Document doc = Jsoup.parse(html);
	    	logger.info(url);

			LinkedHashMap<String, String> mapcre_ins = new LinkedHashMap<String, String>();
			String title = "";
			String down_cnt = "";
			String author = "";
			String author_1st = "";
			String abstract_ = "";
			String author_intro = "";
			String provider_url = url;
			String pub_date = "";
			String pub_year = "";
			String begin_page = "";
			String end_page = "";
			String accept_time = "";
			String meeting_name = "";
					
			//固定字段
			String lngid = "";
			String rawid = "";
			String sub_db_id = "00157";
			String is_oa = "0";
			String sub_db = "HY";
			String product = "ASCE";
			String publisher = "American Society of Civil Engineers";
			String provider = "ASCE";
			
			String doi = "";
			String source_type = "6";
			String country = "US";
			String language = "EN";
			
			String[] temp;
			String authors = "";
			String author_intros = "";
			String base_url = "https://ascelibrary.org/doi";
			
			String year = "";
			String month_days = "";
			String month_day = "";
			String all_time = "";
			String name_place = "";
			
			//智图字段，会议信息
			
			Element editionTag = doc.select("div.conference-details.article-type").first();
			if(editionTag !=null) {
				meeting_name = editionTag.text().trim();
			}
			
            
			//标题
            Element titleTag = doc.select("h1").first();
            if (titleTag !=null) {        
            	title = titleTag.text().trim();
			}
          
            //下载次数
            Element downcntTag = doc.select("div.article-top-region.clearIt > div").first();
            if(downcntTag !=null) {
            	down_cnt = downcntTag.text().replace("Downloaded", "").replace("times", "").trim();
            }
            
            //作者
            for (Element authorTag : doc.select("span.authorName")) {
				author += authorTag.text().trim() + ";";
			}
            
            //第一作者
            if (author !=null) {
            	author_1st = author.split(";")[0];
			}
            
            //会议简介
            if(utmap.containsKey(meeting_record_name)) {
            	meeting_intro = utmap.get(meeting_record_name);
            }
        	
            //摘要
        	Element abstractTag = doc.select("div.NLM_sec.NLM_sec_level_1.hlFld-Abstract > p").first();
            if(abstractTag !=null) {
            	abstract_ = abstractTag.text().trim();
            }
            
            //作者简介
            for (Element author_introTag : doc.select("div.author-block")) {
            	
            	if(author_introTag !=null) {
            		try {
            			authors = author_introTag.select("span.authorName").first().text();
					} catch (Exception e) {
						authors = author_introTag.select("div.authorName").first().text();
					}
            		
                	author_intros = author_introTag.select("div.authorAffiliation").first().text();
                	author_intro += authors + "," + author_intros + ";";
            	}
			}
              
            
            //出版时间
            Element pubTag = doc.select("div.publicationContentEpubDate.dates").first();
            if(pubTag !=null) {
            	pub_date = stdDate(pubTag.text().replace("Published online:", "").trim());
            	//出版年份
            	pub_year = pub_date.substring(0,4).trim();
            }
            
            //开始页码和结束页码
            if(!page_info.equals("")) {
            	try {
            		temp = page_info.split("-");
                	if(temp.length >1) {
                		begin_page = temp[0].trim();
                		end_page = temp[1].trim();
                	}
				} catch (Exception e) {
					// TODO: handle exception
					begin_page = page_info;
				}
            }
            
           //会议时间
            if(!before_time.equals("")) {      	
            	if(!before_time.contains(",")) {
            		Matcher datetype1 = Pattern.compile("(\\d{4})").matcher(meeting_record_name);
                    if(datetype1.find()) {
                    	year = datetype1.group(1);
                    }
                    if(before_time.contains("-")) {
                    	temp = before_time.split("-");
                        month_day = temp[0];
                    }else if(before_time.contains("–")) {
                    	temp = before_time.split("–");
                        month_day = temp[0];
                    }else {
						month_day = before_time;
						
					}
                    all_time = month_day + "," + year;
                    accept_time = stdDate(all_time);
            	}
            	
            	else {
            		temp = before_time.split(",");
            		year = temp[1].trim();
            		month_days = temp[0].trim();
            		if(temp[0].contains(".")) {
            			month_days = temp[0].replace(".", "–");
            		}
            		if(month_days.contains("–")) {
            			temp = month_days.split("–");
            			month_day = temp[0].trim();
            		}else if (month_days.contains("-")) {
            			temp = month_days.split("-");
            			month_day = temp[0].trim();
					}else {
						month_day = month_days;
					}  
            		 all_time = month_day + "," + year;
                     accept_time = stdDate(all_time);
            	}	
            }      
            
            
            
            //固定字段
            doi = provider_url.replace("https://ascelibrary.org/doi/","");
            rawid = doi;
            if (rawid.equals("")) {
            	context.getCounter("map", "error rawid").increment(1);
            	return;
			}
            lngid = VipIdEncode.getLngid(sub_db_id,rawid,false);
            
            
            

			XXXXObject xObj = new XXXXObject();
//			

			xObj.data.put("title",title);
			xObj.data.put("down_cnt",down_cnt);
			xObj.data.put("author",author);
			xObj.data.put("accept_date",accept_time);
			xObj.data.put("author_1st",author_1st);
			xObj.data.put("subject_word",subject_word);
			xObj.data.put("abstract",abstract_);
			xObj.data.put("author_intro",author_intro);
			xObj.data.put("provider_url",provider_url);
			xObj.data.put("pub_date",pub_date);
			xObj.data.put("pub_year",pub_year);
			xObj.data.put("eisbn",eisbn);
			xObj.data.put("isbn",isbn);
			xObj.data.put("page_info", page_info);
			xObj.data.put("begin_page",begin_page);
			xObj.data.put("end_page",end_page);
			xObj.data.put("meeting_name",meeting_name);
			xObj.data.put("meeting_record_name", meeting_record_name);
			xObj.data.put("meeting_intro",meeting_intro);
			xObj.data.put("meeting_place",meeting_place);
			xObj.data.put("meeting_date_raw", before_time);
			xObj.data.put("is_oa", is_oa);
			
			xObj.data.put("lngid",lngid);
			xObj.data.put("rawid",rawid);
			xObj.data.put("sub_db_id",sub_db_id);
			xObj.data.put("sub_db",sub_db);
			xObj.data.put("product",product);
			xObj.data.put("publisher",publisher);
			xObj.data.put("provider",provider);
			xObj.data.put("down_date",down_date);
			xObj.data.put("batch",batch);
			xObj.data.put("doi",doi);
			xObj.data.put("source_type",source_type);
			xObj.data.put("country",country);
			xObj.data.put("language",language);
			
					
				
			context.getCounter("map", "count").increment(1);

			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(rawid), new BytesWritable(bytes));
            
	    }

	}
}