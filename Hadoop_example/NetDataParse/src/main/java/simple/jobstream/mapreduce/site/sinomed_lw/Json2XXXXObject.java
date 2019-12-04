package simple.jobstream.mapreduce.site.sinomed_lw;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
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
	    	Map<String, String> mapJson = null; 
	    	try {
	    		mapJson = gson.fromJson(value.toString(), type);
			} catch (Exception e) {
				// TODO: handle exception
				return;
			}
	    	
	    	String rawid = mapJson.get("rawid").trim();
	    	String provider_url = mapJson.get("provider_url").trim();
	    	String title = mapJson.get("title").trim();
	    	String author = mapJson.get("author").trim();
	    	String pub_date = mapJson.get("pub_date").trim();
	    	String degree = mapJson.get("degree").trim();
	    	String organ = mapJson.get("organ").trim();	 
	    	String contributor = mapJson.get("contributor").trim();
	    	String html = mapJson.get("htmlText").trim();
	    	
			Document doc = Jsoup.parse(html);						
            
			LinkedHashMap<String, String> mapcre_ins = new LinkedHashMap<String, String>();
			
			String information = "";
			String title_alt = "";
			String down_date = "20191112";
			String keyword = "";
			String pub_year = "";
			String page_cnt = "";
			String abstract_ = "";
			String abstract_alt = "";
			String clc_no = "";
			String research_field = "";
			
			String abinfo = "";
			String abinfo_en = "";
			
			String lngid = "";
			String product = "SINOMED";
			String sub_db = "XW"; 
			String provider = "sinomedthesis";
			String sub_db_id = "00184";
			String source_type = "4";
			String country = "CN";
			String language = "ZH";
			
            lngid = VipIdEncode.getLngid(sub_db_id,rawid,false);
            
            if(pub_date != "") {
            	pub_year = pub_date.substring(0, 4).trim();
            }else {
            	pub_date = "19000000";
            	pub_year = "1900";
            }
            
			Elements div = doc.select("div.lab-txt.fL.w100");
			for (Element p : div) {
				for (Element pTag : p.select("p")) {
					information = pTag.text().trim();
					if(information.startsWith("英文标题:")) {
						title_alt = information.replace("英文标题: ", "").trim();
					}
					if(information.startsWith("页码:")) {
						page_cnt = information.replace("页码: ", "").trim();
					}
					if(information.startsWith("其他导师:")) {
						contributor = contributor + ";" + information.replace("其他导师: ", "");
						contributor = contributor.replace(" ", ";").replace("、", ";").replace("，", ";").replace(",", ";").trim();
					}
					if(information.startsWith("研究专业:")) {
						research_field = information.replace("研究专业:", "").trim();
					}
					if(information.startsWith("关键字:")) {
						keyword = information.replace("关键字: ", "").trim();
						if(!keyword.contains(";")) {
							keyword = keyword.replace(" ", ";");
						}
						keyword = keyword.replace(",", ";").replace("，", ";").replace("  ", "").replace("、", ";").replace("?", "").replace("\r", "").replace("\n", "");
						if(keyword.startsWith(";")) {
							int len = keyword.length();
							keyword = keyword.substring(1,len);
						}
					}
					if(information.startsWith("中文摘要:")) {
						abinfo = information.replace("中文摘要: ", "").replace("?", "").trim();
						if(abinfo.contains("<p>")) {
							Document a = Jsoup.parse(abinfo);
							Elements pp = a.select("p");
							for(Element ppTag : pp) {
								abstract_ += ppTag.text().trim();
							}
						}else {
							abstract_ = abinfo;
						}
						
					}
					if(information.startsWith("英文摘要:")) {
						abinfo_en = information.replace("英文摘要: ", "").replace("?", "").trim();
						if(abinfo_en.contains("<p>")) {
							Document a_en = Jsoup.parse(abinfo_en);
							Elements pp_en = a_en.select("p");
							for(Element pp_enTag : pp_en) {
								abstract_alt += pp_enTag.text().trim();
							}
						}else {
							abstract_alt = abinfo_en;
						}
					}
					if(information.startsWith("分类号:")) {
						clc_no = information.replace("分类号: ", "").trim();
					}
				}
			}
			Date dt = new Date();
			DateFormat df = new SimpleDateFormat("yyyyMMdd");
			String nowDate = df.format(dt);
			batch = nowDate + "00";
            
			
			
			XXXXObject xObj = new XXXXObject();
			
			xObj.data.put("author",author);
			xObj.data.put("title",title);
			xObj.data.put("organ", organ);
			xObj.data.put("degree", degree);
			xObj.data.put("title_alt",title_alt);
			xObj.data.put("pub_date",pub_date);
			xObj.data.put("pub_year",pub_year);
			xObj.data.put("clc_no",clc_no);
			xObj.data.put("contributor",contributor);
			xObj.data.put("page_cnt",page_cnt);
			xObj.data.put("research_field",research_field);
			xObj.data.put("keyword",keyword);
			xObj.data.put("abstract",abstract_);
			xObj.data.put("abstract_alt",abstract_alt);

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