package simple.jobstream.mapreduce.site.academicjournal;

import java.awt.List;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.block_005finfo_005fxml_jsp;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.TextNode;
import org.jsoup.select.Elements;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;

//将Html格式转化为XXXXObject格式，包含去重合并
public class Html2XXXXObjectacademic extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 30;
	private static int reduceNum = 30;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	//这个目录会被删除重建
  
	public void pre(Job job)
	{
		String jobName = "academicjournalxobj";
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
		job.setReducerClass(ProcessReducer.class);
		
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
		
		static int cnt = 0;	
		
		public static HashMap<String,String> parseHtml(String htmlText){
			HashMap<String,String> map = new HashMap<String,String>();
			String rawid = "";
			String doi="";
			String title = "";
			String identifier_pissn="";
			String identifier_eissn="";
			String creator="";
			String creator_institution = "";
			String source = "";
			String publisher = "";
			String volume = "";
			String issue = "";
			String description = "";
			String date_created = "";
			String subject = "";
			String page ="";
			String temp = "";
			String url = "";
			String years = "";
			String citation_lastpage = "";
			String citation_firstpage = "";
			String description_fund = "";
			
			
			int num = 1; 
			
			
			JsonObject obj = new JsonParser().parse(htmlText).getAsJsonObject();
			String html = obj.get("html").getAsString();
			identifier_pissn = obj.get("issn").getAsString();
			identifier_eissn = obj.get("eissn").getAsString();
			years = obj.get("years").getAsString();
			Document doc = Jsoup.parse(html);
			Element h1 = doc.select("h1.wi-article-title").first();
			if (h1 != null) {
				title = h1.text();
			}
			if(doc.select("meta[name = citation_doi]").first() != null){
				doi = doc.select("meta[name = citation_doi]").first().attr("content");
			}
			if(doc.select("meta[name = citation_firstpage]").first() != null){
				citation_firstpage = doc.select("meta[name = citation_firstpage]").first().attr("content");
			}
			if(doc.select("meta[name = citation_lastpage]").first() != null){
				citation_lastpage = doc.select("meta[name = citation_lastpage]").first().attr("content");
			}
			if (!citation_firstpage.equals("")){
				page = citation_firstpage +"-"+citation_lastpage;
			}
			if (doc.select("meta[name = citation_publisher]").first() != null) {
				publisher = doc.select("meta[name = citation_publisher]").first().attr("content");
			}
			if (doc.select("meta[name = citation_volume]").first() != null) {
				volume = doc.select("meta[name = citation_volume]").first().attr("content");
			}
			if (doc.select("meta[name = citation_issue]").first() != null) {
				issue = doc.select("meta[name = citation_issue]").first().attr("content");
			}
			if (doc.select("meta[name = citation_journal_title]").first() != null) {
				source = doc.select("meta[name = citation_journal_title]").first().attr("content");
			}
			if (doc.select("meta[name = citation_publication_date]").first() != null) {
				date_created = doc.select("meta[name = citation_publication_date]").first().attr("content");
				date_created = date_created.replace("/", "").replace("-", "").replace(".", "");
			}
			if (doc.select("meta[property = og:url]").first() != null) {
				url = doc.select("meta[property = og:url]").first().attr("content");
				
			}
			if (doc.select("meta[name = citation_keyword]") != null) {
				Elements Ekws = doc.select("meta[name = citation_keyword]");
				for(Element e: Ekws){
					subject = subject + e.attr("content") +";";
				}
				subject = subject.replaceAll(";+$", "");
			}
			Elements fund = doc.select("h2[class = section-title]");
			if (fund != null) {
				for (Element e : fund) {
					if (e.text().toLowerCase().equals("funding")) {
						Element pElement = e.nextElementSibling();
						description_fund = pElement.text();
						description_fund = description_fund.replace("'", "''").trim();
					}
				}
			}
			
			
			int i = 1;
			ArrayList creator_institution_list = new ArrayList();
			Element masthead = doc.select("div[class = al-authors-list]").first(); 
			if(masthead!=null){
				Elements lis = masthead.select("span[class = al-author-info-wrap arrow-up]");
				if(lis !=null){
					for(Element span : lis){
						//作者
						Element div = span.select("div[class = info-card-name]").first();
						if (div!=null) {
							temp = div.text();//作者
						}
						
						//机构列表
						Element affilitation = span.select("div[class = info-card-affilitation]").first();
						if (affilitation != null) {
							Elements lis2 = affilitation.select("div[class = aff]");
							if (lis2 != null) {
								String list =""; 
								for(Element e: lis2){
									Element errElement = e.select("div[class = label -label]").first();
									if (errElement != null) {
										errElement.html("");
									}
									String data = e.text();
									if (creator_institution_list.contains(data)) {
										int x = creator_institution_list.indexOf(data)+1;
										list =list + x+",";
										continue;
										}
									creator_institution_list.add(data);
									creator_institution = creator_institution+"["+(num)+"]"+data+";";
									
									i = num;
									num+=1;
									list +=i+",";
									
								}
								if (!list.equals("")) {
									list = list.substring(0,list.length()-1);
									creator = creator+temp+"["+(list)+"]"+";";
								}
								else {
									creator = creator+temp+";";
								}
								
								}
							
							} 
						
					}
					creator_institution = creator_institution.replaceAll(";+$", "");
					creator = creator.replaceAll(";+$", "");
				}
			}
			Element div = doc.select("div.widget-items").first();
			if (div != null) {
				description = div.text();
				
			}
			if (description.startsWith("Abstract")) {
				
				description = description.substring("Abstract".length(),description.length());
			}
			
		rawid = doi;
		
		if (rawid.trim().length() > 0) {			
			map.put("rawid", rawid);
			map.put("doi", doi);
			map.put("title", title);
			map.put("identifier_pissn", identifier_pissn);
			map.put("identifier_eissn", identifier_eissn);
			map.put("creator_institution", creator_institution);
			map.put("creator", creator);
			map.put("source", source);
			map.put("date_created", date_created);
			map.put("volume", volume);
			map.put("issue", issue);
			map.put("description", description);
			map.put("years", years);
			map.put("subject", subject);
			map.put("publisher", publisher);
			map.put("page", page);
			map.put("url", url);
			map.put("citation_firstpage", citation_firstpage);
			map.put("citation_lastpage", citation_lastpage);
			map.put("description_fund", description_fund);
		}
		
		return map;

	}
	
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	cnt += 1;
    	if (cnt == 1) {
			System.out.println("text:" + value.toString());
		}
    	
    	String text = value.toString();
    	HashMap<String, String> map = parseHtml(text);
		if(map.size() < 1){
			context.getCounter("map", "map.size() < 1").increment(1);	
    		return;				
		}
		
		if (map.get("rawid").equals("")) {
			
			context.getCounter("map", "not publisher").increment(1);	
    		return;	
		}
		/*
		if (map.get("title").equals("")) {
			
			context.getCounter("map", "not title").increment(1);	
    		return;	
		}
		*/
		if (map.get("title").equals("")) {
			context.getCounter("map", "not title").increment(1);	
    		return;	
		}
		XXXXObject xObj = new XXXXObject();
		
		xObj.data.put("rawid", map.get("rawid"));
		xObj.data.put("doi",map.get("doi"));
		xObj.data.put("title", map.get("title"));
		xObj.data.put("identifier_pissn",map.get("identifier_pissn"));
		xObj.data.put("identifier_eissn",map.get("identifier_eissn"));
		xObj.data.put("creator", map.get("creator"));
		xObj.data.put("creator_institution", map.get("creator_institution"));
		xObj.data.put("source", map.get("source"));
		xObj.data.put("volume", map.get("volume"));
		xObj.data.put("issue", map.get("issue"));
		xObj.data.put("description", map.get("description"));
		xObj.data.put("years", map.get("years"));
		xObj.data.put("subject", map.get("subject"));
		xObj.data.put("date_created", map.get("date_created"));
		xObj.data.put("publisher", map.get("publisher"));
		xObj.data.put("url", map.get("url"));
		xObj.data.put("page", map.get("page"));
		xObj.data.put("citation_firstpage", map.get("citation_firstpage"));
		xObj.data.put("citation_lastpage", map.get("citation_lastpage"));
		xObj.data.put("description_fund", map.get("description_fund"));
		
		
		byte[] bytes = com.process.frame.util.VipcloudUtil.SerializeObject(xObj);
		context.write(new Text(map.get("rawid")), new BytesWritable(bytes));		
	}
	}
	
	public static class ProcessReducer extends
	Reducer<Text, BytesWritable, Text, BytesWritable> {
	public void reduce(Text key, Iterable<BytesWritable> values, 
	                   Context context
	                   ) throws IOException, InterruptedException {
		
		
		BytesWritable bOut = new BytesWritable();	//用于最后输出
		for (BytesWritable item : values) {
			if (item.getLength() > bOut.getLength()) {	//选最大的一个
				bOut = item;
			}
		}			
		context.getCounter("reduce", "count").increment(1);
	
		context.write(key, bOut);
	}
	}
}
