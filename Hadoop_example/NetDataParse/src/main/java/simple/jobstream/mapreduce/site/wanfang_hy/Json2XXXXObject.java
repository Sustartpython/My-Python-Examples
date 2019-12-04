package simple.jobstream.mapreduce.site.wanfang_hy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.util.StringHelper;
import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class Json2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 10;
	private static int reduceNum = 10;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	//这个目录会被删除重建
  
	public void pre(Job job)
	{
		String jobName = "XXXXObjectHY";
		if (testRun) {
			jobName = "test_" + jobName;
		}
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
		job.setReducerClass(UniqXXXXObjectReducer.class);
		
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
		private static String provider_id="";
		private static String provider ="";
		private static String provider_url="";
		private static String batch="";
		private static String medium ="";
		private static String country="CN";
		private static String description_fund ="";
		private static String subject_clc="";
		private static String type ="";
		private static String language ="ZH";
		private static String lngid="";
		private static String rawid="";
		private static String title="";
		private static String date ="";
		private static String title_series ="";
		private static String creator="";
		private static String creator_institution="";
		private static String source ="";
		private static String source_institution ="";
		private static String subject="";
		private static String description="";
		private static String creator_release="";
		private static String date_created ="";
		private static String day="";
		private static String month="";
		private static String year="";
		private static String day1="";
		private static String month1="";
		private static String year1="";
		private static String years1="";
		private static String years2="";
		private static String host_organ="";
		private static String clc_no_1st="";
		private static String organ_1st="";
		private static String author_1st="";
		// 出版日期
		private static String pub_date="";
		// 会议日期
		private static String accept_date="";
		public void setup(Context context) throws IOException, InterruptedException {
			batch = context.getConfiguration().get("batch");
			context.getCounter("batch", batch).increment(1);
		}
		
		
		static int cnt = 0;	
		
		public void parseHtml(String htmlText){
			
			{// 老字段
				pub_date="";
				accept_date="";
				provider_id ="";
				provider ="";
				provider_url="";
				batch="";
				medium ="";
				country="CN";
				description_fund ="";
				subject_clc="";
				type ="";
				language ="ZH";
				lngid="";
				rawid="";
				title="";
				date ="";
				title_series ="";
				creator="";
				creator_institution="";
				source ="";
				source_institution ="";
				subject="";
				description="";
				creator_release="";
				date_created ="";
				year="";
				day="";
				month="";
				host_organ="";
				clc_no_1st="";
				organ_1st="";
				author_1st="";
				year1="";
				day1="";
				month1="";
				years1="";
				years2="";
			}	

			int idx = htmlText.indexOf('★');

			rawid = htmlText.substring(0,idx);	
			Document doc = Jsoup.parse(htmlText);
			try{
				title  = doc.select("h1").text();
			}catch(Exception e){
				title = "";
			}

			try{
				description = doc.select("meta[name = description]").first().attr("content");
				description= description.substring(2).replace("\n", "").replace("\t", "").replace("\r", "");
			}catch(Exception e){
				description = "";
			}
			System.out.println(description);
			try{
				subject = doc.select("meta[name = keywords]").first().attr("content");
				subject = subject.replace(" ", ";");
			}catch(Exception e){
				subject = "";
			}

			Element masthead = doc.select("div[class=fixed-width-wrap fixed-width-wrap-feild]").first();
			try{
				Element zuoz = doc.select("div[class=row row-author]").first();
				Elements links = zuoz.getElementsByTag("a");
				for (Element link : links) {
						 String linkText = link.text();
						 creator  = creator +linkText+";";
					}
				}catch (Exception e) {
					// TODO: handle exception
					creator = "";
				}
			try{
				Element div = masthead.select("div[class=fixed-width baseinfo-feild]").first();
				Elements divs = div.getElementsByTag("div");
				for (Element data : divs) {
					
					if(data.select("span[class=pre]").text().equals("作者单位：")){
						// 获取该标签下的span
						Elements datespan = data.select("span[class=text] > span");
						System.out.println("1111111111111");
						if(datespan != null) {
						for (Element onespan : datespan) {
							creator_institution = creator_institution+ onespan.text().replaceAll(";$", "").trim() +";";
						}
						creator_institution = creator_institution.replaceAll(";$","");
			
						}}
					else if(data.select("span[class=pre]").text().equals("母体文献：")){
						title_series = data.select("span[class=text]").text().trim();
					}
					
					else if(data.select("span[class=pre]").text().equals("会议名称：")){
						source = data.select("span[class=text]").text().trim();
					}
					
					else if(data.select("span[class=pre]").text().equals("会议时间：")){		
						years1 = data.select("span[class=text]").text().replace("年", "-").replace("月", "-").replace("日", "").trim();
						String[] list = years1.split("-");
						if (list.length == 3) {
							year = list[0];
							month = list[1];
							day = list[2];
						}
						else if (list.length == 2) {
							year = list[0];
							month = list[1];
							day = "00";
						}
						else if (list.length == 1) {
							year = list[0];
							if (year.equals("")) {
								year = "1900";
							}
							month = "00";
							day = "00";
						}
						else if (list.length == 0) {
							year = "1900";
							month = "00";
							day = "00";
						}
						if(month.length() == 1){
							month = "0" + month;
						}
						if(day.length() == 1){
							day = "0" + day;
						}
						accept_date = year + month + day;
						if (accept_date.length() > 8) {
							accept_date= accept_date.substring(0,8);
						}
						if (accept_date.length()<4) {
							accept_date= "19000000";
						}
					}
					
					else if(data.select("span[class=pre]").text().equals("会议地点：")){
						source_institution = data.select("span[class=text]").text();
					}
					else if(data.select("span[class=pre]").text().equals("主办单位：")){
						creator_release = data.select("span[class=text]").text();
					}
					else if(data.select("span[class=pre]").text().equals("分类号：")){
						subject_clc = data.select("span[class=text]").text();
						subject_clc = subject_clc.replace('\0', ' ').replace("+", "").replace("，", ";").replace(" ", ";").trim();
					}
					else if(data.select("span[class=pre]").text().equals("在线出版日期：")){
						years2 = data.select("span[class=text]").text().replace("年", "-").replace("月", "-").replace("日", "");

						String[] list = years2.split("-");
						if (list.length == 3) {
		
							year1 = list[0];
							month1 = list[1];
							day1 = list[2];
						}
						else if (list.length == 2) {

							year1 = list[0];
							month1 = list[1];
							day1 = "00";
						}
						else if (list.length == 1) {
			
							year1 = list[0];
							if (year1.equals("")) {
								year1 = "1900";
							}
							month1 = "00";
							day1 = "00";
						}
						else if (list.length == 0) {

							year1 = "1900";
							month1 = "00";
							day1 = "00";
						}

						if(month1.length() == 1){
							month1 = "0" + month1;
						}
						if(day1.length() == 1){
							day1 = "0" + day1;
						}

						pub_date = year1 + month1 + day1;

						if (pub_date.length() > 8) {
							pub_date= pub_date.substring(0,8);
						}
						if (pub_date.length()<4) {
							pub_date= "19000000";
						}
						
					}
				}
			}catch(Exception e){
				e.printStackTrace();
			}
			System.out.println(creator_institution);
			// 处理date
			if(accept_date.length() <1) {
				date="1900";
				accept_date="19000000";
				
			}else {
				date =  accept_date.substring(0,4);
			}
			if(pub_date.length() <1) {
				pub_date="19000000";
			}

			
//			if(pub_date.length() >0 && accept_date.length() >0) {
//				date =pub_date.substring(0,4);
//			}
//			else if (pub_date.length() <0 && accept_date.length() >0) {
//				pub_date = accept_date;
//				date =  accept_date.substring(0,4);
//			}else if (pub_date.length() >0 && accept_date.length() <0) {
//				accept_date = pub_date;
//				date =  pub_date.substring(0,4);
//			}else if (pub_date.length() <1 && accept_date.length() <1) {
//				date="1900";
//				pub_date="19000000";
//				accept_date="19000000";
//			}
	
		}
		
	    public void map(LongWritable key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	    	cnt += 1;
	    	if (cnt == 1) {
				System.out.println("text:" + value.toString());
			}
	    	
	    	String text = value.toString();
	    	int idx = text.indexOf('★');
	    	if (idx < 1) {
	    		context.getCounter("map", "not find ★").increment(1);	
	    		return;
			}

	    	parseHtml(text);
			context.getCounter("map", "countAll").increment(1);
			if (title.length() < 1) {
				context.getCounter("map", "no title").increment(1);
				return;
			}
			// 剔除rawid为空的数据
			if (rawid.length() < 1) {
				context.getCounter("map", "no rawid").increment(1);
				return;
			}
			lngid = VipIdEncode.getLngid("000105", rawid, false);
			provider_url ="http://www.wanfangdata.com.cn/details/detail.do?_type=conference&id="+rawid;
			
			if(subject_clc.contains(";")) {
				clc_no_1st =subject_clc.split(";")[0];			
			}
			else {
				if(subject_clc.length() >0) {
					clc_no_1st =subject_clc;
				}				
			}	
			
			// 处理第一机构
			if(creator_institution.contains(";")) {
				if (creator_institution.split(";").length <1) {
					creator_institution="";
					organ_1st ="";
					
					context.getCounter("map", "onle ;号").increment(1);
				}else {
					String news = "";
					List<String> list = Arrays.asList(creator_institution.split(";"));
					Set<String> set = new HashSet<String>(list);
					List<String> result = new ArrayList<String>(set);
					for (int i = 0; i < result.size(); i++) {
						news = news + ";" + result.get(i);
					}
					organ_1st = creator_institution.split(";")[0];
					creator_institution = news.replaceAll("^;", "").trim();
					organ_1st =creator_institution.split(";")[0];	
				}					
			}
			else {
				if(creator_institution.length() >0) {
					organ_1st=creator_institution;
				}				
			}
			
			// 处理第一作者
			creator = creator.replaceAll("； ", ";").replaceAll("；", ";").replaceAll("；$", "").replaceAll(";$", "");
			if(creator.contains(";")) {
				
				if(creator.split(";").length<1) {
					creator ="";
					author_1st="";
				}
				else {
					author_1st =creator.split(";")[0];	
				}								
			}
			else {
				if(creator.length() >0) {
					author_1st=creator;
				}				
			}
			// 去除关键字中多余的;号
			subject =StringHelper.cleanSemicolon(subject);
			
			// 处理作者问题
	
			XXXXObject xObjOut = new XXXXObject();
			{
				xObjOut.data.put("lngid",lngid);
				xObjOut.data.put("rawid",rawid);
				xObjOut.data.put("sub_db_id","000105");
				xObjOut.data.put("product","WANFANG");
				xObjOut.data.put("sub_db","CCPD");
				xObjOut.data.put("provider","WANFANG");
				xObjOut.data.put("down_date","20190617");
				xObjOut.data.put("batch",batch);
				xObjOut.data.put("doi","");
				xObjOut.data.put("source_type","6");
				xObjOut.data.put("provider_url",provider_url);
				xObjOut.data.put("title",title);
				xObjOut.data.put("title_alt","");
				xObjOut.data.put("title_sub","");
				xObjOut.data.put("keyword",subject);
				xObjOut.data.put("keyword_alt","");
				xObjOut.data.put("keyword_machine","");
				xObjOut.data.put("clc_no_1st",clc_no_1st);
				xObjOut.data.put("clc_no",subject_clc);
				xObjOut.data.put("clc_machine","");
				xObjOut.data.put("subject_edu","");
				xObjOut.data.put("subject","");
				xObjOut.data.put("abstract",description);
				xObjOut.data.put("abstract_alt","");
				xObjOut.data.put("begin_page","");
				xObjOut.data.put("end_page","");
				xObjOut.data.put("jump_page","");
				xObjOut.data.put("accept_date",accept_date);
				xObjOut.data.put("pub_date",accept_date);
				xObjOut.data.put("pub_date_alt",pub_date);
				xObjOut.data.put("pub_place","");
				xObjOut.data.put("page_cnt","");
				xObjOut.data.put("pdf_size","");
				xObjOut.data.put("fulltext_addr","");
				xObjOut.data.put("fulltext_type","");
				xObjOut.data.put("fund",description_fund);
				xObjOut.data.put("author_id","");
				xObjOut.data.put("author_1st",author_1st);
				xObjOut.data.put("author",creator);
				xObjOut.data.put("author_alt","");
				xObjOut.data.put("corr_author","");
				xObjOut.data.put("corr_author_id","");
				xObjOut.data.put("research_field","");
				xObjOut.data.put("author_intro","");
				xObjOut.data.put("organ_id","");
				xObjOut.data.put("organ_1st",organ_1st.replaceAll("；$", ""));
				xObjOut.data.put("organ",creator_institution.replaceAll("；$", ""));
				xObjOut.data.put("organ_alt","");
				xObjOut.data.put("host_organ",creator_release.replace(",",";").replace("、", ";").replaceAll(";$","").trim());
				xObjOut.data.put("host_organ_id","");
				xObjOut.data.put("sponsor","");
				xObjOut.data.put("organ_area","");
				xObjOut.data.put("pub_year",date);
				xObjOut.data.put("vol","");
				xObjOut.data.put("num","");
				xObjOut.data.put("is_suppl","");
				xObjOut.data.put("issn","");
				xObjOut.data.put("eissn","");
				xObjOut.data.put("cnno","");
				xObjOut.data.put("publisher","");
				xObjOut.data.put("meeting_name",source);
				xObjOut.data.put("meeting_name_alt","");
				xObjOut.data.put("meeting_record_name",title_series);
				xObjOut.data.put("meeting_record_name_alt","");
				xObjOut.data.put("meeting_place",source_institution);
				xObjOut.data.put("meeting_counts","");
				xObjOut.data.put("meeting_code","");
				xObjOut.data.put("is_oa","");
				xObjOut.data.put("country",country);
				xObjOut.data.put("language",language);
			}
			context.getCounter("map", "count").increment(1);

			byte[] bytes = VipcloudUtil.SerializeObject(xObjOut);
			context.write(new Text(rawid), new BytesWritable(bytes));		
		}
	}

}
