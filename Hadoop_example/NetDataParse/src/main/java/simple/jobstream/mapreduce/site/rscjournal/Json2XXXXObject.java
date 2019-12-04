package simple.jobstream.mapreduce.site.rscjournal;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.LinkedHashMap;
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

//统计wos和ei的数据量
public class Json2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 5;
	private static int reduceNum = 5;
	
	public static  String inputHdfsPath = "";
	public static  String outputHdfsPath = "";
	
	public void pre(Job job)
	{
		String jobName = "rscjournal." + this.getClass().getSimpleName();
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

	public void SetMRInfo(Job job) {
		job.getConfiguration().set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));
		
		job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(ProcessReducer.class);

		TextOutputFormat.setCompressOutput(job, false);

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

	// ======================================处理逻辑=======================================
	//继承Mapper接口,设置map的输入类型为<Object,Text>
	//输出类型为<Text,IntWritable>
	public static class ProcessMapper extends Mapper<LongWritable, Text, Text, BytesWritable> {
		
		
		public void setup(Context context) throws IOException,
			InterruptedException {
		}
		
		
		
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	
	    	Gson gson = new Gson();
	    	Type type = new TypeToken<Map<String, String>>(){}.getType();
	    	Map<String, String> mapJson = gson.fromJson(value.toString(), type);
	    	
	    	String url = mapJson.get("url").trim();
	    	String pissn = mapJson.get("pissn").trim();
	    	String eissn = mapJson.get("eissn").trim();
	    	String gch = mapJson.get("gch").trim();
	    	String html  = mapJson.get("detail").trim();
	    	
			Document doc = Jsoup.parse(html);						
            
            String creator ="";
            String title = "";
            String date_created = "";
            String provider_subject="";
            String description="";
            String doi="";
            String beginpage="";
            String endpage="";
            String insitution="";
            String source="";
            String subject="";
            String publisher="";
            String volume="";
            String issue="";
            String corr_author="";
            String rawtype="";

            
            Element titleTag = doc.select("meta[name*=DC.title]").first();
            
            if (titleTag !=null) {        
            	title = titleTag.attr("content");
			}

            Element doiTag = doc.select("meta[name*=citation_doi]").first();
            if (doiTag !=null) {        
            	doi = doiTag.attr("content");
			}
            Element sourceTag = doc.select("meta[name*=citation_journal_title]").first();
            if (sourceTag !=null) {        
            	source = sourceTag.attr("content");
			}
            Element subjectTag = doc.select("meta[name*=keywords]").first();
            if (subjectTag !=null) {        
            	subject = subjectTag.attr("content");
			}
            Element publisherTag = doc.select("meta[name*=DC.publisher]").first();
            if (publisherTag !=null) {        
            	publisher = publisherTag.attr("content").trim();
			}
            Element issnTag = doc.select("meta[name=citation_issn]").first();
            if (issnTag !=null) {        
            	pissn = issnTag.attr("content");
			}
            
            
            Element descriptionTag = doc.select("div.capsule__text").first();
            if (descriptionTag !=null) {
            	description = descriptionTag.text();
			}
            
            Element dateTag = doc.select("meta[name*=DC.issued]").first();
            if (dateTag !=null) {
            	date_created = dateTag.attr("content").replace("/", "");
			}
            
            Element beginPageTag = doc.select("meta[name*=citation_firstpage]").first();
            if (beginPageTag !=null) {
            	beginpage = beginPageTag.attr("content");
			}
            
            Element endPageTag = doc.select("meta[name*=citation_lastpage]").first();
            if (endPageTag !=null) {
            	endpage = endPageTag.attr("content");
			}
            
            Element volTag = doc.select("meta[name*=citation_volume]").first();
            if (volTag !=null) {
            	volume = volTag.attr("content");
			}
            
            Element issueTag = doc.select("meta[name*=citation_issue]").first();
            if (issueTag !=null) {
            	issue = issueTag.attr("content");
            	if(issue.equals("0")){
            		issue = "";
            	}
			}
            
            for (Element authorTag : doc.select("div.article__authors > span")){
            	String supString = "";
            	for (Element supTag : authorTag.select("sup")){
            		supString = supString + supTag.text() + ",";
            	}
            	
            	String author = authorTag.select("a").first().text();
            	
            	Element spanTag = authorTag.select("span").first();
            	if (spanTag.text().trim().equals("*")) {
					corr_author = corr_author + author + ";";
				}
            	if (!supString.equals("")) {         		
            		author = author + "[" + supString.substring(0, supString.length()-1) + "]";
				}

            	creator = creator + author + ";";
            }
            
            if (creator.length()>0) {
            	creator = creator.substring(0, creator.length()-1);
			}
            
            if (corr_author.length()>0) {
            	corr_author = corr_author.substring(0, corr_author.length()-1);
			}
            
            for (Element affiliationTag : doc.select("p.article__author-affiliation")){
            	Element supTag = affiliationTag.select("sup").first();
            	if (supTag !=null) {
//            		affiliationTag.select("a").remove();
//            		affiliationTag.select("b").remove();
            		affiliationTag.select("sup").remove();
            		if (!affiliationTag.text().equals("")) {
            			insitution = insitution + "[" + supTag.text() + "]" + affiliationTag.text() + ";";
					}
//            		else {
//						System.out.println(url);
//					}
				}
            }
            
            if (insitution.length()>0) {
            	insitution = insitution.substring(0, insitution.length()-1);
			}
            
            
            
            
            String[] result = new String[2];
			try {
				result = AuthorOrgan.renumber(creator, insitution);
				creator = result[0];
	            insitution = result[1];
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            
            
            for (Element rawTag : doc.select("div.list__collection--columnised > div.list__item--dashed")){
            	String raw = rawTag.text().trim();
            	if (raw.startsWith("Article type:")) {
					rawtype = raw.replace("Article type:", "").trim();
				}
            }
            
            
                      
			
            

			XXXXObject xObj = new XXXXObject();

			xObj.data.put("url", url);
			xObj.data.put("gch", gch);
			xObj.data.put("title",title);	
			xObj.data.put("creator", creator);
			xObj.data.put("beginpage", beginpage);
			xObj.data.put("endpage", endpage);
			xObj.data.put("date_created", date_created);			
			xObj.data.put("subject", subject);
			xObj.data.put("insitution", insitution);
			xObj.data.put("description", description);
//			xObj.data.put("provider_subject", provider_subject);
			xObj.data.put("volume", volume);
			xObj.data.put("issue", issue);
			xObj.data.put("source", source);
			xObj.data.put("doi", doi);
			xObj.data.put("pissn", pissn);
			xObj.data.put("eissn", eissn);
			xObj.data.put("publisher", publisher);
			xObj.data.put("corr_author", corr_author);
			xObj.data.put("rawtype", rawtype);
			

			context.getCounter("map", "count").increment(1);

			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(doi), new BytesWritable(bytes));
            
	    }

	}
	//继承Reducer接口，设置Reduce的输入类型为<Text,IntWritable>
	//输出类型为<Text,IntWritable>
	public static class ProcessReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
		public void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
			/*
			Text out =  new Text()
			for (BytesWritable val:values){
				VipcloudUtil.DeserializeObject(val.getBytes(), out);
				context.write(key,out);
			}
		//context.getCounter("reduce", "count").increment(1);*/
			BytesWritable bOut = new BytesWritable();	
			for (BytesWritable item : values) {
				if (item.getLength() > bOut.getLength()) {	
					bOut.set(item.getBytes(), 0, item.getLength());
				}
			}
			
			context.getCounter("reduce", "count").increment(1);			
			
			bOut.setCapacity(bOut.getLength()); 	
		
			context.write(key, bOut);
		
			
		}
	}
}