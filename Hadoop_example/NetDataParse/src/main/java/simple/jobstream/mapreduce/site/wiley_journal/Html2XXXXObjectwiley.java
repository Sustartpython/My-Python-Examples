package simple.jobstream.mapreduce.site.wiley_journal;

import java.awt.List;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
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
public class Html2XXXXObjectwiley extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 30;
	private static int reduceNum = 30;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	//这个目录会被删除重建
  
	public void pre(Job job)
	{
		String jobName = "wileyjournal.xxxobj";
		
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
		
		//记录日志到HDFS
		/*
		public boolean log2HDFSForMapper(Context context, String text) {
			Date dt=new Date();//如果不需要格式,可直接用dt,dt就是当前系统时间
			DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");//设置显示格式
			String nowTime = df.format(dt);//用DateFormat的format()方法在dt中获取并以yyyy/MM/dd HH:mm:ss格式显示
			
			df = new SimpleDateFormat("yyyyMMdd");//设置显示格式
			String nowDate = df.format(dt);//用DateFormat的format()方法在dt中获取并以yyyy/MM/dd HH:mm:ss格式显示
			
			text = nowTime + "\n" + text + "\n\n";
			
			boolean bException = false;
			BufferedWriter out = null;
			try {
				// 获取HDFS文件系统  
		        FileSystem fs = FileSystem.get(context.getConfiguration());
		  
		        FSDataOutputStream fout = null;
		        String pathfile = "/vipuser/chenyong/log/log_map/" + nowDate + ".txt";
		        if (fs.exists(new Path(pathfile))) {
		        	fout = fs.append(new Path(pathfile));
				}
		        else {
		        	fout = fs.create(new Path(pathfile));
		        }
		        
		        out = new BufferedWriter(new OutputStreamWriter(fout, "UTF-8"));
			    out.write(text);
			    out.close();
			    
			} catch (Exception ex) {
				bException = true;
			}
			
			if (bException) {
				return false;
			}
			else {
				return true;
			}
		}
		*/
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
			String htmltext = "";
			String citation_lastpage = "";
			String citation_firstpage = "";
			int num = 1; 
			JsonObject obj = new JsonParser().parse(htmlText).getAsJsonObject();
			String html = obj.get("html").getAsString();
			identifier_eissn = obj.get("issn").getAsString();
			Document doc = Jsoup.parse(html);
		
			
			//if(html.lastIndexOf("article-header__title")>-1){
				
			if(doc.select("meta[name = citation_firstpage]").first() != null){
				citation_firstpage = doc.select("meta[name = citation_firstpage]").first().attr("content");
			}
			if(doc.select("meta[name = citation_lastpage]").first() != null){
				citation_lastpage = doc.select("meta[name = citation_lastpage]").first().attr("content");
			}
			if (!citation_firstpage.equals("")){
				page = citation_firstpage +"-"+citation_lastpage;
			}
			if(doc.select("meta[name = citation_doi]").first() != null){
				doi = doc.select("meta[name = citation_doi]").first().attr("content");
			}
			rawid = doi;
			if(doc.select("meta[name = citation_journal_title]").first() != null){
				source = doc.select("meta[name = citation_journal_title]").first().attr("content");
			}
			if(doc.select("meta[name = citation_abstract_html_url]").first() != null){
				url = doc.select("meta[name = citation_abstract_html_url]").first().attr("content");
			}
			if(doc.select("meta[name = citation_volume]").first() != null){
				volume = doc.select("meta[name = citation_volume]").first().attr("content");
			}
			
			if(doc.select("meta[name = citation_issue]").first() != null){
				issue = doc.select("meta[name = citation_issue]").first().attr("content");
			}
			if(doc.select("meta[name = citation_issn]").first() != null){
				identifier_pissn = doc.select("meta[name = citation_issn]").first().attr("content");
			}
			try {
				Elements kws = doc.select("meta[name = citation_keywords]");
				if (kws!=null){
					for(Element e : kws){	
						subject = subject + e.attr("content") +";";
					}
					subject = subject.replaceAll(";+$", "");
				}
			} catch (Exception e) {
				// TODO: handle exception
				subject = "";
			}
			try {
				Element date = doc.select("span.epub-date").first();
				if (date!=null){
					date_created = date.text();
					}
			} catch (Exception e) {
				// TODO: handle exception
			}
			int i = 1;
			try {
				Element masthead = doc.select("div[id = sb-1]").first(); 
				if(masthead!=null){
					Elements lis = masthead.select("div.accordion-tabbed__tab-mobile");
					if(lis !=null){
						for(Element li : lis){
							Element div = li.select("a.author-name").first();
							temp = div.text();
							Elements lis2 = li.select("p");
							int[] score = new int[10];
							for(Element e: lis2){
								String data = e.text();
								if (data.equals("Corresponding Author")){
									continue;
								}
								else if (data.contains("E-mail")) {
									continue;
								}
								else if (data.contains("http://")) {
									continue;
								}
								else if (data.contains("Fax")) {
									continue;
								}
								else if (data.contains("Tel")) {
									continue;
								}
								else if (data.contains("Email")) {
									continue;
								}
								else if (data.contains("@")) {
									continue;
								}
								else if (data.length()<1) {
									continue;
								}
								creator_institution = creator_institution+"["+(num)+"]"+data+"\n";
								score[i]=num; 
								i+=1;
								num+=1; 
							}
							String list =""; 
							for (int j = 0; j < score.length; j++) {
								if (score[j]>0){
									list +=score[j]+",";
								}
								} 
							list = list.substring(0,list.length()-1);
							creator = creator+temp+"["+(list)+"]"+";";
						}
						creator_institution = creator_institution.replaceAll(";+$", "");
						creator = creator.replaceAll(";+$", "");
					}
					
					
				}
			} catch (Exception e) {
				// TODO: handle exception
				creator = "";
				creator_institution = "";
			}
			if (creator.equals("")) {
				try {
					Element masthead = doc.select("div[id = sb-1]").first(); 
					if(masthead!=null){
						Elements lis = masthead.select("div.accordion-tabbed__tab-mobile accordion__closed js--open");
						if(lis !=null){
							for(Element li : lis){
								Element div = li.select("a.author-name accordion-tabbed__control").first();
								temp = div.text();
								Elements lis2 = li.select("p");
								int[] score = new int[10];
								for(Element e: lis2){
									String data = e.text();
									if (data.equals("Corresponding Author")){
										continue;
									}
									else if (data.contains("E‐mail")) {
										continue;
									}
									else if (data.contains("Email")) {
										continue;
									}
									else if (data.contains("http://")) {
										continue;
									}
									else if (data.length()<1) {
										continue;
									}
									else if (data.contains("Fax")) {
										continue;
									}
									else if (data.contains("Tel")) {
										continue;
									}
									else if (data.contains("@")) {
										continue;
									}
									creator_institution = creator_institution+"["+(num)+"]"+data+"\n";
									score[i]=num; 
									i+=1;
									num+=1; 
								}
								String list =""; 
								for (int j = 0; j < score.length; j++) {
									if (score[j]>0){
										list +=score[j]+",";
									}
									} 
								list = list.substring(0,list.length()-1);
								creator = creator+temp+"["+(list)+"]"+";";
							}
							creator_institution = creator_institution.replaceAll(";+$", "");
							creator = creator.replaceAll(";+$", "");
						}
						
						
					}
				} catch (Exception e) {
					// TODO: handle exception
					creator = "";
					creator_institution = "";
				}
				
			}
			
			try {
				
				Element content = doc.select("div.article__body").first();
				if (content!=null){
					Elements links = content.getElementsByTag("p");
					if(links.size()>0){
						for (Element link : links) {
							description = description+link.text();
						}
					}
				}
			} catch (Exception e) {
				// TODO: handle exception
				description = "";
			}
			
			Element aElement = doc.select("div.copyright").first();
			if (aElement != null) {
				publisher = aElement.select("a").first().text();
			}
			if(publisher.equals("")){
				publisher = "John Wiley & Sons, Inc.";
			}
			try {
				Element h1 = doc.select("h1[class = article-header__title]").first();
				title = h1.text();
			} catch (Exception e) {
				// TODO: handle exception
				title = "";
			}
			if (title.equals("")) {
				
				try {
					Element h2 = doc.select("h2[class = citation__title]").first();
					title = h2.text();
				} catch (Exception e) {
					// TODO: handle exception
					title = "";
				}
			}
			
		
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
			map.put("page", page);
			map.put("subject", subject);
			map.put("publisher", publisher);
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
		
		if (map.get("publisher").equals("")) {
			
			context.getCounter("map", "not publisher").increment(1);	
    		return;	
		}
		/*
		if (map.get("title").equals("")) {
			
			context.getCounter("map", "not title").increment(1);	
    		return;	
		}
		*/
		if (map.get("identifier_pissn").equals("")) {
			context.getCounter("map", "not identifier_pissn").increment(1);	
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
		xObj.data.put("page", map.get("page"));

		xObj.data.put("subject", map.get("subject"));
		xObj.data.put("date_created", map.get("date_created"));
		xObj.data.put("publisher", map.get("publisher"));
		
		
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
