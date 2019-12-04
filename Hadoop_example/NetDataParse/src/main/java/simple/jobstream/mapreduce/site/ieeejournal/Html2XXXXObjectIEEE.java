package simple.jobstream.mapreduce.site.ieeejournal;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;

//将Html格式转化为XXXXObject格式，包含去重合并
public class Html2XXXXObjectIEEE extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 40;
	private static int reduceNum = 40;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	//这个目录会被删除重建
  
	public void pre(Job job)
	{
		String jobName = "Html2XXXXObjectIEEE";
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
			String creator="";
			String creator_institution = "";
			String source = "";
			String publisher = "";
			String date = "";
			String volume = "";
			String issue = "";
			String description = "";
			String startpage = "";
			String endpage = "";
			String date_created = "";
			String size = "";
			String subject = "";
			String pubnum = "";
			
			
			Document doc = Jsoup.parse(htmlText);
			try{
				rawid = doc.select("meta[property= og:url]").first().attr("content").split("=")[1].trim();
				
				if(doc.select("meta[name = citation_doi]").first() != null){
					doi = doc.select("meta[name = citation_doi]").first().attr("content");
				}
				if(doc.select("meta[name = citation_title]").first() != null){
					title = doc.select("meta[name = citation_title]").first().attr("content");
				}
				if(doc.select("meta[name = citation_issn]").first() != null){
					identifier_pissn = doc.select("meta[name = citation_issn]").first().attr("content");
				}
				if(doc.select("meta[name = citation_author_institution]").first() != null){
					creator_institution = "[1]" + doc.select("meta[name = citation_author_institution]").first().attr("content");
				}
				Elements authors = doc.select("meta[name = citation_author]");
				if (authors.size()>0){
					
					int i =0;
					for(Element e : authors){
						if (i == 0){
							if(creator_institution != ""){
								creator = e.attr("content") + "[1]" + ";";
							}else{
								creator = e.attr("content") + ";";
							}
						}else{
							creator = creator + e.attr("content") + ";";
						}
						
						i = i + 1;
					}
					
					creator = creator.substring(0, creator.length()-1);
		
				}
				
				if(doc.select("meta[name = citation_journal_title]").first() != null){
					source = doc.select("meta[name = citation_journal_title]").first().attr("content");
				}
				
				if(doc.select("meta[name = citation_publisher]").first() != null){
					publisher = doc.select("meta[name = citation_publisher]").first().attr("content");
				}
				
				if(doc.select("meta[name = citation_date]").first() != null){
					date_created = doc.select("meta[name = citation_date]").first().attr("content");
				}
				if(doc.select("meta[name = citation_volume]").first() != null){
					volume = doc.select("meta[name = citation_volume]").first().attr("content");
				}
				
				if(doc.select("meta[name = citation_issue]").first() != null){
					issue = doc.select("meta[name = citation_issue]").first().attr("content");
				}
				
				if(doc.select("meta[property = og:description]").first() != null){
					description = doc.select("meta[property = og:description]").first().attr("content");
				}
				
				if(doc.select("meta[name = citation_firstpage]").first() != null){
					startpage = doc.select("meta[name = citation_firstpage]").first().attr("content");
				}
				
				if(doc.select("meta[name = citation_lastpage]").first() != null){
					endpage = doc.select("meta[name = citation_lastpage]").first().attr("content");
				}
				
				if(doc.select("meta[name = citation_keywords]").first() != null){
					subject = doc.select("meta[name = citation_keywords]").first().attr("content");
					subject = subject.trim().replaceAll(";\\s*", ";");
				}
				
				if(doc.select("a[href~=\\S*punumber\\S*]").first()!=null){
					pubnum = doc.select("a[href~=\\S*punumber\\S*]").first().attr("href").split("=")[1].trim();
					
				}
			}catch(Exception e){
				e.printStackTrace();
			}
			
			if (rawid.trim().length() > 0) {			
				map.put("rawid", rawid);
				map.put("doi", doi);
				map.put("title", title);
				map.put("identifier_pissn", identifier_pissn);
				map.put("creator_institution", creator_institution);
				map.put("creator", creator);
				map.put("source", source);
				map.put("publisher", publisher);
				map.put("date_created", date_created);
				map.put("volume", volume);
				map.put("issue", issue);
				map.put("description", description);
				map.put("startpage", startpage);
				map.put("endpage", endpage);
				map.put("subject", subject);
				map.put("pubnum", pubnum);
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
	    	int idx = text.indexOf('★');
	    	if (idx < 1) {
	    		context.getCounter("map", "not find ★").increment(1);	
	    		return;
			}
	    	String html = text.substring(idx);

	    	HashMap<String, String>map = parseHtml(html);
			if(map.size() < 1){
				context.getCounter("map", "map.size() < 1").increment(1);	
	    		return;				
			}
			
			XXXXObject xObj = new XXXXObject();
			
			xObj.data.put("rawid", map.get("rawid"));
			xObj.data.put("doi",map.get("doi"));
			
			xObj.data.put("title", map.get("title"));
			xObj.data.put("identifier_pissn",map.get("identifier_pissn"));
			xObj.data.put("creator", map.get("creator"));
			
			xObj.data.put("creator_institution", map.get("creator_institution"));
			xObj.data.put("source", map.get("source"));
			xObj.data.put("publisher", map.get("publisher"));
			xObj.data.put("volume", map.get("volume"));
			xObj.data.put("issue", map.get("issue"));
			xObj.data.put("description", map.get("description"));
			xObj.data.put("startpage", map.get("startpage"));
			xObj.data.put("endpage", map.get("endpage"));
			xObj.data.put("subject", map.get("subject"));
			xObj.data.put("punumber",map.get("pubnum"));
			xObj.data.put("date_created", map.get("date_created"));
			xObj.data.put("endpage", map.get("endpage"));
			
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
