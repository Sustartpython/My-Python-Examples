package simple.jobstream.mapreduce.site.cssci;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import simple.jobstream.mapreduce.common.vip.AuthorOrgan;
import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;
import simple.jobstream.mapreduce.site.cssci.CssciDetail;
import simple.jobstream.mapreduce.site.cssci.CssciDetail.Author;
import simple.jobstream.mapreduce.site.cssci.CssciDetail.Catation;
import simple.jobstream.mapreduce.site.cssci.CssciDetail.Contents;
import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.util.StringHelper;

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
	// 继承Mapper接口,设置map的输入类型为<Object,Text>
	// 输出类型为<Text,IntWritable>
	public static class ProcessMapper extends Mapper<LongWritable, Text, Text, BytesWritable> {
		public String batch = "";
		private  static Map<String, String> maptype =new HashMap<String, String>();
		private  static Map<String, String> mapxktype =new HashMap<String, String>();
		private static void initMapType() {
			maptype.put("1", "论文");
			maptype.put("2", "综述");
			maptype.put("3", "评论");
			maptype.put("5", "报告");
			maptype.put("4", "传记资料");
			maptype.put("9", "其他");
		}
		
		private static void initXKType() {
			mapxktype.put("630", "管理学");
			mapxktype.put("850", "民族学");
			mapxktype.put("860", "新闻学与传播学");
			mapxktype.put("870", "图书馆、情报与文献学");
			mapxktype.put("880", "教育学");
			mapxktype.put("890", "体育学");
			mapxktype.put("910", "统计学");
			mapxktype.put("920", "心理学");
			mapxktype.put("930", "社会科学总论");
			mapxktype.put("940", "军事学");
			mapxktype.put("950", "文化学");
			mapxktype.put("960", "人文、经济地理");
			mapxktype.put("970", "环境科学");
			mapxktype.put("840", "社会学");
			mapxktype.put("820", "法学");
			mapxktype.put("710", "马克思主义");
			mapxktype.put("720", "哲学");
			mapxktype.put("730", "宗教学");
			mapxktype.put("740", "语言学");
			mapxktype.put("009", "文学");
			mapxktype.put("751", "外国文学");
			mapxktype.put("752", "中国文学");
			mapxktype.put("760", "艺术学");
			mapxktype.put("770", "历史学");
			mapxktype.put("780", "考古学");
			mapxktype.put("790", "经济学");
			mapxktype.put("810", "政治学");
			mapxktype.put("999", "其他学科");
		}

		public void setup(Context context) throws IOException, InterruptedException {
			batch = context.getConfiguration().get("batch");
			initMapType();
			initXKType();
		}

		static String[] getPage(String text) {
			text = text.replaceAll("-+|\\+-", "-").replaceAll("[;|,|\\-|\\.|\\]|`]$", "").replaceAll("\\s?-\\s", "-");
			String[] result = new String[3]; 
			String begin_page = "";
			String end_page = "";
			String jump_page = "";
			Matcher pagetype1 = Pattern.compile("(\\w+-\\w+)-(\\w+-\\w+),(\\w+-\\w+)").matcher(text);
			Matcher pagetype2 = Pattern.compile("(\\D+-\\w+)[-|,](\\D+-\\w+)").matcher(text);
			Matcher pagetype3 = Pattern.compile("P?(\\w+)-(\\w+)[,|;|\\s|\\.|/](\\w+)$").matcher(text);
			Matcher pagetype4 = Pattern.compile("^P?(\\D)-(\\d+)$").matcher(text);
			Matcher pagetype5 = Pattern.compile("^P?(\\w+)-(\\w+)$").matcher(text);			
			if (pagetype1.find()) {
				begin_page = pagetype1.group(1);
				end_page = pagetype1.group(2);
				jump_page = pagetype1.group(3);
			}
			else if (pagetype2.find()) {
				begin_page = pagetype2.group(1);
				end_page = pagetype2.group(2);
			}
			else if (pagetype3.find()) {
				begin_page = pagetype3.group(1);
				end_page = pagetype3.group(2);
				jump_page = pagetype3.group(3);
			}
			else if (pagetype4.find()) {
				begin_page = text;
				end_page = text;
			}
			else if (pagetype5.find()) {
				begin_page = pagetype5.group(1);
				end_page = pagetype5.group(2);
			}
			else {
				if (!text.contains(",")) {
					begin_page = text;
					end_page = text;
				}			
			}
			result[0] = begin_page;
			result[1] = end_page;
			result[2] = jump_page;
			return result;
		}
		
		

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


			LinkedHashMap<String, String> mapcre_ins = new LinkedHashMap<String, String>();
			String author = "";
			String author_1st = "";
			String organ = "";
			String organ_1st = "";
			String title = "";
			String title_alt = "";
			String keyword = "";
			String pub_year = "";
			String vol = "";
			String num = "";
			String journal_raw_id = "";
			String journal_name = "";
			String raw_type = "";
			String clc_no = "";
			String clc_no_1st = "";
			String subject = "";
			String fund = "";
			String page_info = "";
			String begin_page = "";
			String end_page = "";
			String jump_page = "";
			String ref_cnt = "0";
			
			String lngid = "";
			String rawid = "";
			String product = "CSSCI";
			String sub_db = "QK";
			String provider = "NJU";
			String sub_db_id = "00028";
			String source_type = "3";
			String provider_url = "";
			String country = "CN";
			String language = "ZH";
			String ref_id = "";
			
			Gson gson = new Gson();
			CssciDetail pJson = null;
			Type type = new TypeToken<Map<String, String>>() {
			}.getType();
			Map<String, String> mapJson = gson.fromJson(value.toString(), type);

			String down_date = mapJson.get("down_date").trim();
			String jsonString = mapJson.get("html").trim();
//			String myJson= gson.toJson(value.toString());
			try {
				pJson = gson.fromJson(jsonString, CssciDetail.class);			
			} catch (Exception e) {
				context.getCounter("map", "jsonerror").increment(1);
				return;
				// TODO: handle exception
			}
//			CssciDetail pJson = gson.fromJson(value.toString(), CssciDetail.class);
			
			if (pJson.author!=null) {
				context.getCounter("map", "author").increment(1);
				for (Author creator : pJson.author) {
					String tmp_organ = "";
					if (!creator.jgmc.equals("")) {
						tmp_organ = creator.jgmc;
						if (!creator.bmmc.equals("")) {
							tmp_organ = creator.jgmc+"·"+creator.bmmc;
						}
					}
					else if (!creator.bmmc.equals("")) {
						tmp_organ = creator.bmmc;
					}
		    		if (author_1st.equals("")) {
		    			author_1st = creator.zzmc;
		    			if (organ_1st.equals("")) {
		    				organ_1st = tmp_organ;
						}
					}
		    		mapcre_ins.put(creator.zzmc, tmp_organ);
				}
		    	String[] authororg = AuthorOrgan.numberByMap(mapcre_ins);
		    	author = authororg[0];
		    	organ = authororg[1];
			}			
	    	if (pJson.contents!=null) {
	    		for (Contents content : pJson.contents) {
		    		title =  content.lypm;
		    		title_alt = content.blpm.replace("\\", "");
		    		keyword = content.byc.replace("aaa", "").replaceAll(";$", "");
		    		pub_year = content.nian;
		    		vol = content.juan;
		    		num = content.qi.replaceAll("^0|0$", "");
		    		journal_raw_id = content.qkdm;
		    		journal_name = content.qkmc;
		    		if (maptype.containsKey(content.wzlx)) {
		    			raw_type = maptype.get(content.wzlx);
					}
		    		if (!content.xkdm1.startsWith("***")) {
		    			clc_no_1st = content.xkdm1;
			    		if (!content.xkdm1.equals("") && !content.xkdm2.equals("")) {
			    			clc_no = content.xkdm1 + ";" + content.xkdm2;
						}
			    		else {
			    			clc_no = clc_no_1st;
						}
					}
		    		else if (!content.xkdm2.equals("***")){
		    			clc_no_1st = content.xkdm2;
		    			clc_no = content.xkdm2;
					}
		    		
		    		
	    			if (mapxktype.get(content.xkfl1) != null) {
	    				subject = mapxktype.get(content.xkfl1);
	    				if (mapxktype.get(content.xkfl2) != null){
	    					subject = subject + ";" + mapxktype.get(content.xkfl2);
	    				}
					}
	    			else if (mapxktype.get(content.xkfl2) != null) {
	    				subject = mapxktype.get(content.xkfl2);
					}
					

		    		fund = content.xmlb;
		    		page_info = content.ym;
		    		String[] page = getPage(page_info);
		    		begin_page = page[0];
		    		end_page = page[1];
		    		jump_page = page[2];
//		    		if (page_info.contains("-") && page_info.split("-").length == 2) {
//						begin_page = page_info.split("-")[0];
//						end_page = page_info.split("-")[1];
//					}
		    		rawid = content.sno;
				}
				
			}
	    	else {
	    		context.getCounter("map", "content_null_count").increment(1);
			}
	    	
	    	if (pJson.catation != null) {    		
	    		ref_cnt = String.valueOf(pJson.catation.size());
			}
	    	provider_url = "http://cssci.nju.edu.cn/ly_search_list.html?id=" + rawid;
	    	
	    	lngid = VipIdEncode.getLngid(sub_db_id,rawid,false);

			

			XXXXObject xObj = new XXXXObject();

			xObj.data.put("author",author);
			xObj.data.put("author_1st",author_1st);
			xObj.data.put("organ",organ);
			xObj.data.put("organ_1st",organ_1st);
			xObj.data.put("title",title);
			xObj.data.put("title_alt",title_alt);
			xObj.data.put("keyword",keyword);
			xObj.data.put("pub_year",pub_year);
			xObj.data.put("vol",vol);
			xObj.data.put("num",num);
			xObj.data.put("journal_raw_id",journal_raw_id);
			xObj.data.put("journal_name",journal_name);
			xObj.data.put("raw_type",raw_type);
			xObj.data.put("clc_no",clc_no);
			xObj.data.put("clc_no_1st",clc_no_1st);
			xObj.data.put("subject",subject);
			xObj.data.put("fund",fund);
			xObj.data.put("page_info",page_info);
			xObj.data.put("begin_page",begin_page);
			xObj.data.put("end_page",end_page);
			xObj.data.put("jump_page",jump_page);		
			xObj.data.put("ref_cnt",ref_cnt);
			
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
			xObj.data.put("batch", batch);
			xObj.data.put("down_date", down_date);
			xObj.data.put("ref_id", ref_id);
			

			context.getCounter("map", "count").increment(1);

			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(rawid), new BytesWritable(bytes));

		}

	}
}