package simple.jobstream.mapreduce.site.wanfang_bz;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Map;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class Html2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 20;
	private static int reduceNum = 20;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	//这个目录会被删除重建
  
	public void pre(Job job)
	{
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

	public void SetMRInfo(Job job)
	{
		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.7f);
		System.out.println("******mapred.reduce.slowstart.completed.maps*******" + job.getConfiguration().get("mapred.reduce.slowstart.completed.maps"));
		job.getConfiguration().set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));
		
		
		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(UniqXXXXObjectReducer.class);
		
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(BytesWritable.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(BytesWritable.class);
	    
	    //job.setInputFormatClass(SimpleTextInputFormat.class);
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
		
		public static String batch = "";
		static int cnt = 0;	
		
		//清理的分号和空白
		static String cleanLastSemicolon(String text) {
			text = text.replace('；', ';');			//全角转半角
			text = text.replaceAll("\\s*;\\s*", ";");	//去掉分号前后的空白
			text = text.replaceAll("\\s*\\[\\s*", "[");	//去掉[前后的空白	
			text = text.replaceAll("\\s*\\]\\s*", "]");	//去掉]前后的空白	
			text = text.replaceAll("[\\s;]+$", "");	//去掉最后多余的空白和分号
			
			return text;
		}
		
		//清理class，比如带大括号的情况（xiandaijj201204178:{G445}）
		static String cleanClass(String text) {
			text = text.replace("{", "").replace("}", "").trim();
			
			return text;
		}
		//国家class
		static String getCountrybyString(String text) {
			Dictionary<String,String> hashTable=new Hashtable<String,String>();
			hashTable.put("中国", "CN");
			hashTable.put("英国", "UK");
			hashTable.put("日本", "JP");
			hashTable.put("美国", "US");
			hashTable.put("法国", "FR");
			hashTable.put("德国", "DE");
			hashTable.put("韩国", "KR");
			hashTable.put("国际", "UN");
			
			if (null != hashTable.get(text)) {
				text = hashTable.get(text);
			}
			else {
				text = "UN";
			}	
			
			return text;
		}
		//语言class
		static String getLanguagebyCountry(String text) {
			Dictionary<String,String> hashTable=new Hashtable<String,String>();
			hashTable.put("CN", "ZH");
			hashTable.put("UK", "EN");
			hashTable.put("US", "EN");
			hashTable.put("JP", "JA");
			hashTable.put("FR", "FR");
			hashTable.put("DE", "DE");
			hashTable.put("UN", "UN");
			hashTable.put("KR", "KR");
			text = hashTable.get(text);
			
			return text;
		}

		protected void setup(Context context) throws IOException, InterruptedException {
			batch = context.getConfiguration().get("batch");
		}
		
		
	    public void map(LongWritable key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	    	
	    	String line = value.toString().trim();
	    	Gson gson = new Gson();
	        Type type = new TypeToken<Map<String, JsonElement>>() {}.getType();
	        Map<String, JsonElement> mapField = gson.fromJson(line, type);
	         
	        String url = mapField.get("url").getAsString();
	        String html = mapField.get("html").getAsString();
	        String donwdate = mapField.get("date").getAsString();
	        
	    	Document doc = Jsoup.parse(html);
			//------------------------a 表---------------------------------------------------------------------------
			//标题
			String title = "";
			//英文标题
			String title_alt = "";
			//摘要
			String abstracts = "";
			//标准编号
			String std_no = "";
			//标准类型
			String raw_type="";
			//起草单位
			String organ = "";
			//发布日期
			String pub_date="";
			//状态
			String legal_status = "";
			//强制性标准
			String is_mandatory = "";
			//实施日期
			String impl_date = "";
			//开本页数
			String page_cnt = "";
			//采用关系
			String adopt_relation = "";
			//中图分类号
			String clc_no ="";
			//中国标准分类号
			String ccs_no = "";
			// 国际标准分类号
			String ics_no = "";
			// 国别
			String country = "CN";
			//语言
			String language = "ZH";
			//关键词
			String keyword = "";
			//第二关键词
			String keyword_alt = "";
			
			//非a表数据 兼容旧表需要字段
			String date = "";
			String bznum2 = "";
			
			
			Element baseinfoElement = doc.select("div.left_con_top").first();
			if (baseinfoElement == null) {
				context.getCounter("map", "baseinfoElement == null").increment(1);
				return;
			}
			
			/************************** begin title ************************/
			{
				Element titleElement = baseinfoElement.select("div.title").first();
				Element title2Element = baseinfoElement.select("div[class='']").first();
				if (titleElement != null) {	//标题
					title = titleElement.text().trim();
				}
				if (title2Element != null) {
					title_alt = title2Element.text().trim();
				}
				// http://www.wanfangdata.com.cn/details/detail.do?_type=standards&id=ISO 7611:1985
				
//				if ((title.length() < 1) && (title_alt.length() < 1)) {
//					throw new InterruptedException("no title :"+url+":"+baseinfoElement);
////					context.getCounter("map", "no title").increment(1);
////					return;
//				}
				//System.out.println("title_c:" + title_c);
				//System.out.println("title_e:" + title_e);
			}
			/************************** end title ************************/
			
			/************************** begin 摘要 ************************/
			{
				Element remarkCElement = baseinfoElement.select("div.abstract > textarea").first();
				if (remarkCElement != null) {
					abstracts = remarkCElement.text().trim();
				}
				//System.out.println("remark_c:" + remark_c);
				//System.out.println("remark_e:" + remark_e);
			}
			/************************** end 摘要 ************************/
			// info 信息在 title 里 http://www.wanfangdata.com.cn/details/detail.do?_type=standards&id=ASTM%20F885-84(2017)
			// 只有title 没有info http://www.wanfangdata.com.cn/details/detail.do?_type=standards&id=ASTM%20C140-2012a
			// 可以搜索到 但无法进详情页 http://www.wanfangdata.com.cn/details/detail.do?_type=standards&id=YB/T 9009-1998
			Element baseinfoFieldElement = doc.select("ul.info").first();
			String text=""; 
			if (baseinfoFieldElement == null) {
				context.getCounter("map", "info == null").increment(1);
				throw new InterruptedException("no info :"+url+":"+baseinfoElement);
			}
			if (baseinfoFieldElement != null) {
				for (Element rowElement : baseinfoFieldElement.select("li")) {
					try {
					text = rowElement.select("> div.info_left").first().text().trim();
					}catch (Exception e) {
						// TODO: handle exception
						text = rowElement.select("> a > div.info_left").first().text().trim();
					}
					if (text.startsWith("标准编号：")) {
						std_no = rowElement.select("> div.info_right.author").text().trim();
//						if (identifier_standard.toUpperCase().startsWith("GB")) {
//							bzmaintype = "国家标准";
//						}	
					}
//					else if (text.startsWith("发布单位：")) {
//						creator_release = rowElement.select("> span.text").text().trim();
//						//System.out.println("creator_release:" + creator_release);
//					}
					else if (text.startsWith("标准类型：")) {
						raw_type = rowElement.select("> div.info_right.author").text().trim();
					//System.out.println("creator_release:" + creator_release);
					}
					else if (text.startsWith("起草单位：")) {
						organ = rowElement.select("> div.info_right.info_right_newline").text().trim();
					}
					else if (text.startsWith("发布日期：")) {
						pub_date = rowElement.select("> div.info_right.author").text().trim().replace("-","");
						//System.out.println("date_created:" + date_created);
					}
					else if (text.startsWith("状态：")) {
						legal_status = rowElement.select("> div.info_right.author").text().trim();
					}
					
					else if (text.startsWith("强制性标准：")) {
						is_mandatory = rowElement.select("> div.info_right.author").text().trim();
						//System.out.println("page:" + page);
					}
					else if (text.startsWith("实施日期：")) {
						impl_date = rowElement.select("> div.info_right.author").text().trim().replace("-","");
						//System.out.println("date_impl:" + date_impl);
					}
					else if (text.startsWith("开本页数：")) {
						page_cnt = rowElement.select("> div.info_right.author").text().trim();
						//System.out.println("page:" + page);
					}
					else if (text.startsWith("采用关系：")) {
						adopt_relation = rowElement.select("> div.info_right.author").text().trim();
						//System.out.println("subject_clc:" + subject_clc);
					}
					else if (text.startsWith("中图分类号：")) {
						clc_no = rowElement.select("> div.info_right.author").text().trim();
						//System.out.println("subject_clc:" + subject_clc);
					}
					else if (text.startsWith("中国标准分类号：")) {
						ccs_no = rowElement.select("> div.info_right.author").first().text().trim();
						//System.out.println("subject_csc:" + subject_csc);
					}
					else if (text.startsWith("国际标准分类号：")) {
						ics_no = rowElement.select("> div.info_right.author").text().trim();
						//System.out.println("subject_isc:" + subject_isc);
					}
					else if (text.startsWith("关键词：")) {
						keyword = rowElement.select("> div.info_right.info_right_newline").text().trim();
						//System.out.println("keyword_c:" + keyword_c);
					}
					else if (text.startsWith("Keyword：")) {
						keyword_alt = rowElement.select("> div.info_right.info_right_newline").text().trim();
						//System.out.println("keyword_e:" + keyword_e);
					}
				}
			}
		
			
			
//			//获取国家和语言
//			country = getCountrybyString(country);
//			System.out.println("new country:" + country);
//
//			language = getCountrybyString(country);
//			System.out.println("language:" + language);
	
			
			//获取日期和年份
			if (pub_date.isEmpty()) {
				pub_date = "19000000";
			}
			date = pub_date.substring(0,4);
			
			//获取bznum2
			bznum2 = std_no.replaceAll("[^a-z^A-Z^0-9]", "");
			
			String rawid = std_no;
			if (title.equals("")) {
				title = std_no;
			}
			
			if (rawid.equals("")) {
				context.getCounter("map", "rawid is null").increment(1);
				return ;
			} 
			
			abstracts = Jsoup.parse(abstracts).text().trim();
			
			XXXXObject xObj = new XXXXObject();
			xObj.data.put("rawid", rawid);
			//标题
			xObj.data.put("title", title);
			//英文标题
			xObj.data.put("title_alt", title_alt);
			//摘要
			xObj.data.put("abstracts", abstracts);
			//标准编号
			xObj.data.put("std_no", std_no);
			//标准类型
			xObj.data.put("raw_type", raw_type);
			//起草单位
			xObj.data.put("organ", organ);
			//发布日期
			xObj.data.put("pub_date", pub_date);
			//状态
			xObj.data.put("legal_status", legal_status);
			//强制性标准
			xObj.data.put("is_mandatory", is_mandatory);
			//实施日期
			xObj.data.put("impl_date", impl_date);
			//开本页数
			xObj.data.put("page_cnt", page_cnt);
			//采用关系
			xObj.data.put("adopt_relation", adopt_relation);
			//中图分类号
			xObj.data.put("clc_no", clc_no);
			//中国标准分类号
			xObj.data.put("ccs_no", ccs_no);
			// 国际标准分类号
			xObj.data.put("ics_no", ics_no);
			// 国别
			xObj.data.put("country", country);
			//语言
			xObj.data.put("language", language);
			//关键词
			xObj.data.put("keyword", keyword);
			//第二关键词
			xObj.data.put("keyword_alt", keyword_alt);
			
			xObj.data.put("batch", batch);
			xObj.data.put("down_date", donwdate);
			
			xObj.data.put("url", url);
			
			//非a表数据 兼容旧表需要字段
			xObj.data.put("date", date);
			xObj.data.put("bznum2", bznum2);

			
			context.getCounter("map", "count").increment(1);
			
			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(rawid), new BytesWritable(bytes));			
		}
	}
	

}
