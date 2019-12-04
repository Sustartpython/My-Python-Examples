package simple.jobstream.mapreduce.site.rscbook;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;












import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.BXXXXObject;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.SimpleTextInputFormat;
import com.process.frame.util.VipcloudUtil;

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class Html2xxxxobject extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 1;
	private static int reduceNum = 1;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	//这个目录会被删除重建
  
	public void pre(Job job)
	{
		String jobName = "Html2XXXXObject";
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
		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.7f);
		System.out.println("******mapred.reduce.slowstart.completed.maps*******" + job.getConfiguration().get("mapred.reduce.slowstart.completed.maps"));
		job.getConfiguration().set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));


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
		
		 public static HashMap<String,String> parseRSCBookHtml(String htmlText){
		    	String rawid = "";

				String title = "";
				String title_sub = "";
				String title_series = "";
				String identifier_eisbn = "";
				String identifier_pisbn = "";
				String identifier_doi = "";
				String creator = "";
				String creator_bio = "";
				String source_fl = "";
				String publisher = "";
				String date_created = "";
				String date = "";
				String volume = "";
				String description = "";
				String subject = "";
				String page = "";
				HashMap<String,String> map = new HashMap<String,String>();
				Document doc = Jsoup.parse(htmlText);
				if(htmlText.contains("title")){
					
					Element titleElement = doc.select("meta[name = citation_title]").first();
					if(titleElement != null){
						title = titleElement.attr("content");

					}
					
					Element titleSeriesElement = doc.select("div[class = seriescolor fixpadv--xl]").first();
					if(titleSeriesElement != null){
						title_series = titleSeriesElement.text().replace("From the book series:", "").trim();

					}

					Element date_createdElement = doc.select("meta[name = citation_publication_date]").first();
					if(date_createdElement != null){
						date_created = date_createdElement.attr("content").trim().replace("/", "");
						if(date_created.length() > 4){
							date = date_created.substring(0,4);
						}
					}

					
					Element doiElement = doc.select("meta[name = citation_doi]").first();
					if(doiElement != null){
						identifier_doi = doiElement.attr("content");
					}
					
					Element pisbnElement = doc.select("meta[name = citation_isbn]").first();
					if(pisbnElement != null){
						identifier_pisbn = pisbnElement.attr("content").trim();
						rawid = identifier_pisbn;
						
					}
					
					Element eisbnElement = doc.select("dd[id = abstract-about-book-online-isbn]").first();
					if(eisbnElement != null){
						identifier_eisbn = eisbnElement.text().trim();
					}
					
					Element descriptionElement = doc.select("meta[name = description]").first();
					if(descriptionElement != null){
						description = descriptionElement.attr("content").trim();
					}
					
					Element publisherElement = doc.select("dd[id = abstract-about-publisher]").first();
					if(publisherElement != null){
						publisher = publisherElement.text().trim();
					}
					
					Element subjectElement = doc.select("meta[name = keywords]").first();
					if(subjectElement != null){
						subject = subjectElement.attr("content").trim();

					}
					
					Elements creatorElements = doc.select("span[class = page-head__book-authors]");
					
					if(creatorElements != null){
						creator = creatorElements.text().trim();
						creator = creator.replace("Editors:", "").replace("Author:", "").replace(",",";").replace("Authors:", "").replace("Editor:", "").trim();

						
					}
					Elements infoElements = doc.select("div[class = fixpadv--l]");
					if (infoElements != null){
						for(Element infoEle : infoElements){
							String tempString = infoEle.text().trim();
							if(tempString.contains("About this book")){
								description = tempString.replace("About this book", "").trim();
							}
							if(tempString.contains("Author information")){
								creator_bio = tempString.replace("Author information", "").trim();
							}
						}
					}
					
					Element pageElement = doc.select("div[class = text--caption]").last();
					
					if(pageElement != null){
						String tempString = pageElement.text();
						if(tempString.contains("pages")){
							page = pageElement.text().trim().replace("This book contains", "").trim();
							page = page.replace("pages.","").trim();
						}

			
					}
					
					Elements tempElements = doc.select("div[class = list__item--dashed]");
					
					if(tempElements != null){
						for(Element tempEle:tempElements){
							String tempString = tempEle.text().trim();
							if(tempString.contains("PDF eISBN:")){
								identifier_eisbn = tempString.replace("PDF eISBN:", "").trim();
							}
						}
						

			
					}
					
					

					
					
					
				}else{
					map = null;
				}
			

				

			


				map.put("rawid", rawid);
				map.put("title", title);
				map.put("title_series", title_series);
				map.put("identifier_doi", identifier_doi);
				map.put("description", description);
				map.put("date", date);
				map.put("date_created", date_created);
				map.put("publisher", publisher);
				map.put("identifier_pisbn",identifier_pisbn);
				map.put("identifier_eisbn", identifier_eisbn);
				map.put("subject", subject);
				map.put("creator", creator);
				map.put("creator_bio", creator_bio);
				map.put("page", page);
				return map;
			}

		

		
	    public void map(LongWritable key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	    	cnt += 1;
	    	if (cnt == 1) {
				System.out.println("text:" + value.toString());
			}
	    	
	    	HashMap<String, String> map = new HashMap<String, String>();
	    	map = parseRSCBookHtml(value.toString());
	    	

	    	if(map != null){
				XXXXObject xObj = new XXXXObject();
				xObj.data.put("title", map.get("title"));
				xObj.data.put("rawid", map.get("rawid"));
				xObj.data.put("title_series", map.get("title_series"));
				xObj.data.put("identifier_doi", map.get("identifier_doi"));
				xObj.data.put("date_created", map.get("date_created"));
				xObj.data.put("date", map.get("date"));
				xObj.data.put("publisher", map.get("publisher"));
				xObj.data.put("identifier_pisbn", map.get("identifier_pisbn"));
				xObj.data.put("identifier_eisbn", map.get("identifier_eisbn"));
				xObj.data.put("subject", map.get("subject"));
				xObj.data.put("page", map.get("page"));
				xObj.data.put("creator", map.get("creator"));
				xObj.data.put("creator_bio", map.get("creator_bio"));
				xObj.data.put("description", map.get("description"));
				byte[] bytes = com.process.frame.util.VipcloudUtil.SerializeObject(xObj);
				context.write(new Text(map.get("rawid")), new BytesWritable(bytes));	
				context.getCounter("map", "count").increment(1);
	    	}else{
	    		context.getCounter("map", "null").increment(1);
	    		return;
	    	}
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
