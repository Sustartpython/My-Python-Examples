package simple.jobstream.mapreduce.site.ascejournal;

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
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;
import org.mockito.internal.matchers.And;

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
	    	
	    	String vol = mapJson.get("vol").trim();
	    	String num = mapJson.get("num").trim();
	    	String journal_raw_id = mapJson.get("journal_raw_id").trim();
	    	String issn = mapJson.get("issn").trim();
	    	String eissn = mapJson.get("eissn").trim();
	    	String journal_name = mapJson.get("journal_name").trim();
	    	String html = mapJson.get("detail").trim();
	    	
			Document doc = Jsoup.parse(html);						
            
			LinkedHashMap<String, String> mapcre_ins = new LinkedHashMap<String, String>();
			String base_url = "https://ascelibrary.org/doi/";
			String title = "";
			String down_cnt ="";
			String accept_date = "";
			String recv_date = "";
			String pub_date = "";
			String author = "";
			String author_1st = "";
			String subject_word = "";
			String pub_year = "";
			String provider_url = "";
			String abstract_ = "";
			String author_intro = "";
			String keyword = "";
			
			String authors = "";
			String author_intros = "";
			
			String doi = "";
			String lngid = "";
			String rawid = "";
			String product = "ASCE";
			String sub_db = "QK"; 
			String provider = "ASCE";
			String sub_db_id = "00092";
			String source_type = "3";
			String publisher ="American Society of Civil Engineers";
			String country = "US";
			String language = "EN";
			String down_date = "20190825";
			
			
			
			 //采集源url
            Element urlTag = doc.select("div.publicationContentDoi > a").first();
            if(urlTag !=null) {
            	provider_url =base_url + urlTag.text().replace("https://doi.org/", "");
            }
        
            doi = provider_url.replace("https://ascelibrary.org/doi/","");
            rawid = doi;
            if (rawid.equals("")) {
            	context.getCounter("map", "error rawid").increment(1);
            	return;
			}
            lngid = VipIdEncode.getLngid(sub_db_id,rawid,false);
			
			//标题
            Element titleTag = doc.select("div.publicationContentTitle").first();
            if (titleTag !=null) {        
            	title = titleTag.text();
			}
            
            //下载次数
            Element downcntTag = doc.select("div.article-top-region.clearIt > div").first();
            if(downcntTag !=null) {
            	down_cnt = downcntTag.text().replace("Downloaded", "").replace("times", "").trim();
            }
            
            //作者
            for (Element authorTags : doc.select("div.author-block")) {
				Element auTag = authorTags.select("div.authorName > a > span > span").first();
				if(auTag !=null) {
					author += auTag.text() + ";";
				}else {
					Element auaTag = authorTags.select("div.authorName").first();
					if(auaTag !=null) {
						author += auaTag.text() + ";";
					}
				}
			}
            logger.info(provider_url);
            logger.info(author);
            //第一作者
            if (!author.equals("") && !author.equals(";")) {
            	author_1st = author.split(";")[0];
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
            }else {
				Element pubsTag = doc.select("div.article-meta-byline > section > div > a").first();
				if(pubsTag!=null) {
					String sss = pubsTag.text().trim();
					Matcher datetype11 = Pattern.compile("\\((.*?)\\)").matcher(sss);
		            if(datetype11.find()) {
		            	pub_date = stdDate(datetype11.group(1));
		            	pub_year = pub_date.substring(0,4).trim();
		            	
		            }
				}
			}
            
            //接受时间
            Element accTag = doc.select("div.publicationContentAcceptedDate.dates").first();
            if(accTag !=null) {
            	accept_date = stdDate(accTag.text().replace("Accepted: ", "").trim());
            }
            
           //修订时间
            Element recvTag = doc.select("div.publicationContentReceivedDate.dates").first();
            if(recvTag !=null) {
            	recv_date = stdDate(recvTag.text().replace("Received: ", "").trim());
            }
                       
            
            for(Element subTags : doc.select("div.article-meta-byline > section > a")) {
            	if(subTags !=null) {
            		keyword = keyword + subTags.text() + ';' ;
            	}
            }
            
           
			XXXXObject xObj = new XXXXObject();

			xObj.data.put("author",author);
			xObj.data.put("author_1st",author_1st);
			xObj.data.put("title",title);
			xObj.data.put("doi",doi);
			xObj.data.put("accept_date",accept_date);
			xObj.data.put("pub_date",pub_date);
			xObj.data.put("pub_year",pub_year);
			xObj.data.put("subject_word",subject_word);
			xObj.data.put("issn",issn);
			xObj.data.put("eissn",eissn);
			xObj.data.put("abstract",abstract_);
			xObj.data.put("keyword", keyword);
			xObj.data.put("publisher",publisher);
			xObj.data.put("down_cnt", down_cnt);
			xObj.data.put("journal_name", journal_name);
			xObj.data.put("vol", vol);
			xObj.data.put("num", num);
			xObj.data.put("journal_raw_id", journal_raw_id);
			
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