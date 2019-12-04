package simple.jobstream.mapreduce.site.pishuinfo;

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
import simple.jobstream.mapreduce.common.vip.VipIdEncode;

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

			String rawid = mapJson.get("article_id").trim();
			String down_date = "";
			if (mapJson.containsKey("down_date")) {
				down_date = mapJson.get("down_date").trim();
			}
			else {
				down_date = "20190326";
			}
			
			String url = "https://www.pishu.com.cn/skwx_ps/initDatabaseDetail?siteId=14&contentId=" + rawid;

			String html = mapJson.get("detail").trim();

			String creator ="";
            String title = "";
            String date_created = "";
            String date = "";
            String description="";
            String description_en="";

            String subject="";
            String subject_en="";
            String creator_bio="";
            String publisher="社会科学文献出版社";
            String source="";
            String title_series="";
            String pagecount="";
            
			String product = "PISHU";
			String sub_db = "ZX";
			String provider = "SSAP";
			String source_type = "14";
			String provider_url = "";
			String country = "CN";
			String language = "ZH";           
            String sub_db_id="00058";
            
            String lngid = VipIdEncode.getLngid(sub_db_id,rawid,false);



			Document doc = Jsoup.parse(html);
			Element titleTag = doc.select("div.main_right.fr.margintop20 > div.zl_bookIntro > h3").first();            
            if (titleTag !=null) {        
            	title = titleTag.text().trim();
			}
            for (Element spanTag : doc.select("div.main_right.fr.margintop20 > div.zl_bookIntro > ul > li > span")) {
            	String spanStr = spanTag.text();
//            	System.out.println(spanStr);
            	if (spanStr.startsWith("作者：")) {
					creator = spanStr.replace("作者：", "").trim().replace(" ", ";");
				}
            	else if (spanStr.startsWith("出版日期：")) {
            		date_created = spanStr.replace("出版日期：", "").trim().replace("年","").replace("月","");
            		if (date_created.length() == 6) {
            			date_created = date_created + "00";
            		}
            		date = date_created.substring(0, 4).trim();
				}
            	else if (spanStr.startsWith("报告页数：")) {
            		pagecount = spanStr.replace("报告页数：","").replace("页","").replace("&nbsp;","").trim();					
				}
            	else if (spanStr.startsWith("所属丛书：")) {
            		title_series = spanTag.select("a").first().attr("title");
				}
    
			}
            for (Element liTag : doc.select("div.main_right.fr.margintop20 > div.zl_bookIntro > ul > li")) {
            	String liStr = liTag.text();
            	if (liStr.startsWith("所属图书：")) {
            		source = liTag.select("a").first().attr("title");
				}   
            }
            
            Element descriptionTag = doc.select("div[class=summaryCon]").first();
            if (descriptionTag!=null) {
            	description = descriptionTag.text().replaceAll("<<$", "").replaceAll(">>$", "");
            }
            
            Element description_enTag = doc.select("div[class=en_summaryCon]").first();
            if (description_enTag!=null) {
            	description_en = description_enTag.text().replaceAll("<<$", "").replaceAll(">>$", "");
            }
            
            for (Element divTag : doc.select("div[class=zl_keywords]")) {
            	String divStr = divTag.text().trim();
            	
            	if (divStr.startsWith("关键词：")) {
            		for (Element aTag : divTag.select("tr > td > a")) {
            			subject = subject + aTag.text().trim() + ";";
            		}
            		subject = StringHelper.cleanSemicolon(subject);
				}
            	else if (divStr.startsWith("Keywords：")) {
            		for (Element aTag : divTag.select("tr > td > a")) {
            			subject_en = subject_en + aTag.text().trim() + ";";
            		}
            		subject_en = StringHelper.cleanSemicolon(subject_en);
				}
            	
            }
            for (Element divTag : doc.select("div[class=zh_summaryCon] > div")) {
            	String divstr = divTag.text().trim();
                if (divstr.indexOf("暂无简介") == -1) {
                	creator_bio = creator_bio + divstr + "\n";
                }
            }            	              	                  
            creator_bio = creator_bio.trim();

			creator = StringHelper.cleanSemicolon(creator);


			XXXXObject xObj = new XXXXObject();

			xObj.data.put("provider_url", url);
			xObj.data.put("author", creator);
			xObj.data.put("author_intro", creator_bio);
			xObj.data.put("author_1st",creator.split(";")[0]);
			xObj.data.put("pub_date", date_created);
			xObj.data.put("pub_year",date);
			xObj.data.put("title", title);
			xObj.data.put("journal_name", source);
			xObj.data.put("publisher", publisher);
			xObj.data.put("keyword", subject);
			xObj.data.put("keyword_alt", subject_en);
			xObj.data.put("abstract", description);
			xObj.data.put("abstract_alt", description_en);
			xObj.data.put("page_cnt", pagecount);
			xObj.data.put("title_series", title_series);
			
			
			xObj.data.put("lngid",lngid);
			xObj.data.put("rawid",rawid);
			xObj.data.put("product",product);
			xObj.data.put("sub_db",sub_db);
			xObj.data.put("provider",provider);
			xObj.data.put("sub_db_id",sub_db_id);
			xObj.data.put("source_type",source_type);
			xObj.data.put("country",country);
			xObj.data.put("language",language);	
			xObj.data.put("batch", batch);
			xObj.data.put("down_date", down_date);

			context.getCounter("map", "count").increment(1);

			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(rawid), new BytesWritable(bytes));

		}

	}

}