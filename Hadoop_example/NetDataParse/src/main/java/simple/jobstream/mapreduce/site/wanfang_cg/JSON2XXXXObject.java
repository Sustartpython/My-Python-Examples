package simple.jobstream.mapreduce.site.wanfang_cg;

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
import org.jsoup.select.Elements;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.util.StringHelper;
import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class JSON2XXXXObject extends InHdfsOutHdfsJobInfo {
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
		
		static int cnt = 0;	
		public static String batch = "";
		
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
			Type type = new TypeToken<Map<String,Object>>(){}.getType();

			Map<String, Object> mapField = gson.fromJson(line, type);		
			
			if (mapField.get("html") == null) {
				context.getCounter("map", "html is null").increment(1);
				return;
			}
			//标题
			String title = "";
			//项目年度编号
			String std_no="";
			//限制使用
			String restricted="";
			//省市
			String organ_area = "";
			//中图分类号
			String clc_no = "";
			//成果类别
			String raw_type ="";
			//成果公布年份
			String pub_year ="";
			//关键词
			String keyword = "";
			//成果简介
			String abstracts ="";
			//推荐部门
			String recommend_organ="";
			//完成单位
			String organ = "";
			//完成人：
			String author = "";
			//应用行业名称
			String trade_name ="";
			//应用行业码
			String trade_no ="";
			//联系单位名称
			String corr_organ ="";
			//联系人
			String corr_author="";
			//联系单位地址
			String corr_organ_addr = "";
			//传真
			String fax = "";
			//邮政编码
			String postcode = "";
			//rawid
			String rawid="";
			//鉴定部门
			String identify_organ="";
			//鉴定日期
			String identify_date = "";
			//电子邮件
			String email = "";
			//登记号
			String register_no = "";
			//工作起止时间
			String from_to_date = "";
			//专利授权号：
			String authorization_no = "";
			//转让方式
			String transfer_form = "";
			//投资金额
			String investment_amount  = "";
			//产值
			String output_value  = "";
			//转让范围
			String transfer_scope = "";
			//计划名称
			String plan_name = "";
			//转让费
			String transfer_fee ="";
			//列入时间
			String accept_date="";
			//推广跟踪：
			String spread_track="";
			//利税
			String tax =""; 
			//创汇
			String earn_foreign = "";
			//推广范围
			String spread_scope = "";
			//专利申请号
			String app_no="";
			//推广方式
			String spread_form = "";
			//推广情况说明
			String spread_explain="";
			//推荐登记号
			String recommend_no="";
			//推荐日期
			String recommend_date ="";
			//转让注释
			String transfer_annotation="";
			//登记日期
			String register_date = "";
			//投资注释
			String investment_annotation ="";
			//转让内容
			String transfer_content ="";
			//申报单位名称
			String applicant_organ ="";
			//专利项数
			String patent_cnt = "";
			//转让条件
			String transfer_terms = "";
			//投资说明
			String investment_explain = "";
			//建设期
			String build_duration = "";
			//节资
			String save_money = "";
			//学科分类号
			String subject_edu = "";
			//申报日期
			String app_date = "";
			
			
			//下面的为后续补充 没有加入A表字段
			//登记部门
			String register_organ = "";
			
			
			/**
			 	sud_db_id: 00047
				product: WANFANG
				sub_db: CG
				provider: WANFANG
				智图provider: wanfangcstad
			 * */
			
			String downdate = mapField.get("downdate").toString();
			String html = mapField.get("html").toString();
			
			Document doc = Jsoup.parse(html);
			
			try {
				//标题
				title = doc.select("div.title").first().text().trim();
			}catch (Exception e) {
//				// TODO: handle exception
//				throw new InterruptedException("没有标题："+std_no+";"+html);
				return ;
			}
			
			Elements divEles = doc.select("div.achievements");
			for (Element achive:divEles) {
				Elements infoEles = achive.select("ul.info > li");
				if (!infoEles.isEmpty()) {
					for (Element liEle: infoEles) {
						String info_left = liEle.select("div.info_left").text().trim();
						if (info_left.startsWith("项目年度编号：")) {
							std_no  = liEle.select("div.info_right.author").text().trim();
						} else if (info_left.startsWith("限制使用：")) {
							restricted  = liEle.select("div.info_right.author").text().trim();
						} else if (info_left.startsWith("省市：")) {
							organ_area  = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("中图分类号：")) {
							clc_no = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("成果类别：")) {
							raw_type = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("公布年份：")) {
							pub_year = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("成果公布年份：")) {
							pub_year = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("关键词：")) {
							keyword = liEle.select("div.info_right.info_right_newline").text().trim();
						}else if (info_left.startsWith("成果简介：")) {
							abstracts = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("推荐部门：")) {
							recommend_organ = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("完成单位：")) {
							organ  = liEle.select("div.info_right.info_right_newline").text().trim();
						}else if (info_left.startsWith("完成人：")) {
							author  = liEle.select("div.info_right.info_right_newline").text().trim();
						}else if (info_left.startsWith("应用行业名称：")) {
							trade_name = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("应用行业码：")) {
							trade_no = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("联系单位名称：")) {
							corr_organ = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("联系人：")) {
							corr_author = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("联系单位地址：")) {
							corr_organ_addr = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("传真：")) {
							fax = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("邮政编码：")) {
							postcode = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("鉴定部门：")) {
							identify_organ = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("鉴定日期：")) {
							identify_date = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("电子邮件：")) {
							email = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("登记号：")) {
							register_no = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("工作起止时间：")) {
							from_to_date = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("专利授权号：")) {
							authorization_no = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("转让方式：")) {
							transfer_form = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("投资金额：")) {
							investment_amount = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("产值：")) {
							output_value = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("转让范围：")) {
							transfer_scope = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("计划名称：")) {
							plan_name = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("转让费：")) {
							transfer_fee = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("列入时间：")) {
							accept_date = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("推广跟踪：")) {
							spread_track = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("利税：")) {
							tax = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("创汇：")) {
							earn_foreign = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("推广范围：")) {
							spread_scope = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("专利申请号：")) {
							app_no = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("推广方式：")) {
							spread_form = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("推广情况说明：")) {
							spread_explain = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("推荐登记号：")) {
							recommend_no = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("推荐日期：")) {
							recommend_date = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("转让注释：")) {
							transfer_annotation = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("登记日期：")) {
							register_date = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("投资注释：")) {
							investment_annotation = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("转让内容：")) {
							transfer_content = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("申报单位名称：")) {
							applicant_organ = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("专利项数：")) {
							patent_cnt = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("转让条件：")) {
							transfer_terms = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("投资说明：")) {
							investment_explain = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("建设期：")) {
							build_duration = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("节资：")) {
							save_money = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("学科分类号：")) {
							subject_edu = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("申报日期：")) {
							app_date = liEle.select("div.info_right.author").text().trim();
						}else if (info_left.startsWith("登记部门：")) {
							register_organ = liEle.select("div.info_right.author").text().trim();
						}
						else {
							if ("".equals(info_left)){
								
							}else {
							throw new InterruptedException("出现没有解析的字段："+info_left+";"+std_no+";"+liEle);
							}
						}
					}
				}
				
			}
			

			clc_no = clc_no.replace("[", "").replace("]", "").replace(",", ";");
			keyword = keyword.replace(" ", ";");
			
			clc_no = StringHelper.cleanSemicolon(clc_no);
			keyword = StringHelper.cleanSemicolon(keyword);
			
			rawid = std_no;
			
			XXXXObject xObj = new XXXXObject();
			xObj.data.put("title", title);
			xObj.data.put("std_no", std_no);
			xObj.data.put("restricted", restricted);
			xObj.data.put("organ_area", organ_area);
			xObj.data.put("clc_no", clc_no);
			xObj.data.put("raw_type", raw_type);
			xObj.data.put("pub_year", pub_year);
			xObj.data.put("keyword", keyword);
			xObj.data.put("abstracts", abstracts);
			xObj.data.put("recommend_organ", recommend_organ);
			xObj.data.put("organ", organ);
			xObj.data.put("trade_name", trade_name);
			xObj.data.put("trade_no", trade_no);
			xObj.data.put("corr_organ", corr_organ);
			xObj.data.put("corr_organ_addr", corr_organ_addr);
			xObj.data.put("fax", fax);
			xObj.data.put("postcode", postcode);
			xObj.data.put("rawid", rawid);
			xObj.data.put("author", author);
			xObj.data.put("corr_author", author);
			xObj.data.put("identify_organ", identify_organ);
			xObj.data.put("identify_date", identify_date);
			xObj.data.put("email", email);
			xObj.data.put("register_no", register_no);
			xObj.data.put("from_to_date", from_to_date);
			xObj.data.put("authorization_no", authorization_no);
			xObj.data.put("transfer_form", transfer_form);
			xObj.data.put("investment_amount", investment_amount);
			xObj.data.put("output_value", output_value);
			xObj.data.put("transfer_scope", transfer_scope);
			xObj.data.put("plan_name", plan_name);
			xObj.data.put("transfer_fee", transfer_fee);
			xObj.data.put("accept_date", accept_date);
			xObj.data.put("spread_track", accept_date);
			xObj.data.put("tax", tax);
			xObj.data.put("earn_foreign", earn_foreign);
			xObj.data.put("spread_scope", spread_scope);
			xObj.data.put("app_no", app_no);
			xObj.data.put("spread_form", spread_form);
			xObj.data.put("spread_explain", spread_explain);
			xObj.data.put("recommend_no", recommend_no);
			xObj.data.put("recommend_date ", recommend_date);
			xObj.data.put("transfer_annotation ", transfer_annotation);
			xObj.data.put("register_date ", register_date);
			xObj.data.put("investment_annotation", investment_annotation);
			xObj.data.put("transfer_content", transfer_content);
			xObj.data.put("applicant_organ", applicant_organ);
			xObj.data.put("patent_cnt", patent_cnt);
			xObj.data.put("transfer_terms", transfer_terms);
			xObj.data.put("investment_explain", investment_explain);
			xObj.data.put("build_duration", build_duration);
			xObj.data.put("save_money", save_money);
			xObj.data.put("subject_edu", subject_edu);
			xObj.data.put("app_date", app_date);
			xObj.data.put("down_date", downdate);
			xObj.data.put("register_organ", register_organ);
			
			xObj.data.put("batch", batch);

			
			context.getCounter("map", "count").increment(1);
			
			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(rawid), new BytesWritable(bytes));			
		}
	}
}
