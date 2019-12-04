package simple.jobstream.mapreduce.site.naturejournal;

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
public class json2xobj extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 40;
	private static int reduceNum = 40;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	//这个目录会被删除重建
  
	public void pre(Job job)
	{
		String jobName = "nature.xxxobj";
		
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
			String num = "";
			String vol="";
			String title="";
			String product="nature";
			String sub_db="";
			String doi = "";
			String issn = "";
			String pub_date = "";
			String url = "";
			String publisher = "";
			String pdf_url  = "";
			String journal_name = "";
			String author = "";
			String organ = "";
			String DOI = "";
			String abs = "";
			String keyword = "";
			String date = "";
			String journal_name_alt = "";
			String ISSN = "";
			String recv_date = "";
			String accept_date = "";
			String rawid = "";
			String startpage = "";
			String subject = "";
			String endpage = "";
			String batch = (new SimpleDateFormat("yyyyMMdd")).format(new Date());
			JsonObject obj = null;
			try {
				obj = new JsonParser().parse(htmlText).getAsJsonObject();
			} catch (Exception e) {
				return map;
				// TODO: handle exception
			}
			
			String html = obj.get("html").getAsString();
			rawid = obj.get("id").getAsString();
			Document doc = Jsoup.parse(html);
			
			//title
			Element h1 = doc.select("h1[class = tighten-line-height small-space-below]").first();
			if (h1 != null) {
				title = h1.text();
			}
			//vol
			Element citation_volume = doc.select("meta[name = prism.volume]").first();
			if (citation_volume!= null) {
				vol = citation_volume.attr("content");
			}
			//num
			Element citation_issue = doc.select("meta[name = prism.number]").first();
			if (citation_issue!= null) {
				num = citation_issue.attr("content");
			}
			//子刊ID
			Element journal_id = doc.select("meta[name = journal_id]").first();
			if (journal_id!= null) {
				sub_db = journal_id.attr("content");
			}
			//doi
			Element prism_doi = doc.select("meta[name = prism.doi]").first();
			if (prism_doi!= null) {
				doi = prism_doi.attr("content").replace("doi:", "");
			}
			//issn
			Element citation_issn = doc.select("meta[name = prism.issn]").first();
			if (citation_issn!= null) {
				issn = citation_issn.attr("content");
			}
			//出版时间
			Element citation_online_date = doc.select("meta[name = prism.publicationDate]").first();
			if (citation_online_date!= null) {
				pub_date = citation_online_date.attr("content");
			}
			//html_url
			Element html_url = doc.select("meta[name = citation_fulltext_html_url]").first();
			if (html_url!= null) {
				url = html_url.attr("content");
			}
			//publisher
			Element citation_publisher = doc.select("meta[name = dc.publisher]").first();
			if (citation_publisher!= null) {
				publisher = citation_publisher.attr("content");
			}
			//pdf_url
			Element citation_pdf_url = doc.select("meta[name = citation_pdf_url]").first();
			if (citation_pdf_url!= null) {
				pdf_url = citation_pdf_url.attr("content");
			}
			//jurnal_name
			Element citation_journal_title = doc.select("meta[name = prism.publicationName]").first();
			if (citation_journal_title!= null) {
				journal_name = citation_journal_title.attr("content");
			}
			//作者  机构
			Elements all_li = doc.select("li[itemprop = author]");
			if (all_li != null) {
				int orgaNum = 1; 
				int i = 1;
				ArrayList list = new ArrayList();
				String organtemp = "";
				for (Element li : all_li) {
					int[] score = new int[500];
					Element span_name = li.select("a").first();
					if (span_name != null) {
						Elements spans = li.select("span[itemprop=affiliation]");
						if (spans != null) {
							for (Element element : spans) {
								Element meta = element.select("meta[itemprop=address]").first();
								if (meta != null) {
									organtemp = meta.attr("content").trim().replace("'", "''");
									if (list.indexOf(organtemp)>-1) {
										score[i] = list.indexOf(organtemp)+1; 
//										orgaNum = list.indexOf(organtemp);
//										i+=1;
										continue;
									}
									organ = organ+"["+orgaNum+"]"+organtemp+";";
									score[i]=orgaNum;
									list.add(organtemp);
									i+=1;
									orgaNum+=1; 
								}
								else {
									continue;
								}
								
							}
							String line =""; 
							for (int j = 0; j < score.length; j++) {
								if (score[j]>0){
									line +=score[j]+",";
								}
								
								} 
							if (line.equals("")) {
								author = author+span_name.text()+";";
							}
							else {
								line = line.substring(0,line.length()-1);
								author = author+span_name.text()+"["+(line)+"]"+";";
							}
						}
						
					}
					
				}
				
			}
			//摘要
			Element div = doc.select("div[class = pl20 mq875-pl0 js-collapsible-section]").first();
			if (div!= null) {
				abs = div.text().trim();
			}
			//关键词
			Element cleared = doc.select("div[data-component = article-subject-links]").first();
			if (cleared!= null) {
				
				Elements lis = cleared.select("li");
				if (lis != null) {
					for (Element li : lis) {
						keyword = keyword+li.text().trim()+";";
					}
				}
			}
			
			
			if (keyword.length() != 0 && keyword.endsWith(";")) {
				keyword = keyword.substring(0, keyword.length() - 1);
			    }
			Element timeElement = doc.select("div[class = grid grid-12]").first();
			if (timeElement != null) {
				Elements timedivs = timeElement.select("div");
				if (timedivs != null) {
					for (Element timediv : timedivs) {
						//收稿（提交）日期。
						if (timediv.text().contains("Received")) {
							recv_date = timediv.select("time").attr("datetime");
							recv_date = recv_date.replace("-", "");
						}
						//接受日期
						if (timediv.text().contains("Accepted")) {
							accept_date	 = timediv.select("time").attr("datetime");
							accept_date	 = accept_date	.replace("-", "");
						}
					}
				}
			}
			Element spanfirst = doc.select("span[itemprop=pageStart]").first();
			if (spanfirst != null) {
				startpage = spanfirst.text();
			}
			Element spanend = doc.select("span[itemprop=pageEnd]").first();
			if (spanend != null) {
				endpage = spanend.text();
			}
		
		if (rawid.trim().length() > 0) {			
			map.put("rawid", rawid);
			map.put("doi", doi);
			map.put("title", title);
			map.put("vol", vol);
			map.put("num", num);
			map.put("sub_db", sub_db);
			map.put("doi", doi);
			map.put("issn", issn);
			map.put("pub_date", pub_date);
			map.put("url", url);
			map.put("publisher", publisher);
			map.put("pdf_url", pdf_url);
			map.put("journal_name", journal_name);
			map.put("author", author);
			map.put("organ", organ);
			map.put("abs", abs);
			map.put("keyword", keyword);
			map.put("recv_date", recv_date);
			map.put("accept_date", accept_date);
			map.put("product", product);
			map.put("startpage", startpage);
			map.put("endpage", endpage);
			map.put("subject", subject);
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
		if (map.get("title").equals("")) {
			context.getCounter("map", "not title").increment(1);	
    		return;	
		}
		XXXXObject xObj = new XXXXObject();
		xObj.data.put("rawid", map.get("rawid"));
		xObj.data.put("doi",map.get("doi"));
		xObj.data.put("title", map.get("title"));
		xObj.data.put("vol",map.get("vol"));
		xObj.data.put("num",map.get("num"));
		xObj.data.put("sub_db", map.get("sub_db"));
		xObj.data.put("doi", map.get("doi"));
		xObj.data.put("issn", map.get("issn"));
		xObj.data.put("pub_date", map.get("pub_date"));
		xObj.data.put("url", map.get("url"));
		xObj.data.put("publisher", map.get("publisher"));
		xObj.data.put("pdf_url", map.get("pdf_url"));
		xObj.data.put("journal_name", map.get("journal_name"));
		xObj.data.put("author", map.get("author"));
		xObj.data.put("organ", map.get("organ"));
		xObj.data.put("abs", map.get("abs"));
		xObj.data.put("recv_date", map.get("recv_date"));
		xObj.data.put("accept_date", map.get("accept_date"));
		xObj.data.put("product", map.get("product"));
		xObj.data.put("author", map.get("author"));
		xObj.data.put("endpage", map.get("endpage"));
		xObj.data.put("startpage", map.get("startpage"));
		xObj.data.put("keyword", map.get("keyword"));
		xObj.data.put("subject", map.get("subject"));
		
		
		
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
