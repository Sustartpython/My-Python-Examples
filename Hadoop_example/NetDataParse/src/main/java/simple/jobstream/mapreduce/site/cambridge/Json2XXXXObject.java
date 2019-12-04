package simple.jobstream.mapreduce.site.cambridge;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
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
import simple.jobstream.mapreduce.common.util.StringHelper;
import simple.jobstream.mapreduce.common.vip.AuthorOrgan;

//统计wos和ei的数据量
public class Json2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 5;
	private static int reduceNum = 10;
	
	public static  String inputHdfsPath = "";
	public static  String outputHdfsPath = "";
	
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
		
		public static boolean isNumeric(String str){
		    Pattern pattern = Pattern.compile("[0-9]*");
		    return pattern.matcher(str).matches();   
		}
		

		
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	
	    	Gson gson = new Gson();
	    	Type type = new TypeToken<Map<String, String>>(){}.getType();
	    	Map<String, String> mapJson = gson.fromJson(value.toString(), type);
	    	
	    	String url = mapJson.get("url").trim();
	    	String uid = mapJson.get("uid").trim();
	    	String gch = mapJson.get("gch").trim();
	    	String html  = mapJson.get("detail").trim();
	    	
			Document doc = Jsoup.parse(html);			
			
            String creator ="";
            String title = "";
            String date_created = "";
            String subject="";
            String description="";
            String doi="";
            String beginpage="";
            String endpage="";
            String insitution="";
            String source="";
            String volume="";
            String issue="";
            String publisher="";
            String pissn="";
            String eissn="";
            
            Element titleTag = doc.select("h1[class*=article-title]").first();
            if (titleTag !=null) {        
            	title = titleTag.text();
			}
            Element dateTag = doc.select("meta[name*=citation_publication_date]").first();
            if (dateTag !=null) {        
            	date_created = dateTag.attr("content").replace("/", "").replace("ed", "00");
            	if (date_created.length() == 6) {
            		date_created = date_created + "00";
				}
            	if (!isNumeric(date_created)) {
            		dateTag = doc.select("meta[name*=citation_online_date]").first();
            		if (dateTag !=null) {        
                    	date_created = dateTag.attr("content").replace("/", "").replace("ed", "00");
                    	if (date_created.length() == 6) {
                    		date_created = date_created + "00";
                    	}
            		}
            	}
			}
            Element bgPageTag = doc.select("meta[name*=citation_firstpage]").first();
            if (bgPageTag !=null) {        
            	beginpage = bgPageTag.attr("content");
			}
            Element edPageTag = doc.select("meta[name*=citation_lastpage]").first();
            if (edPageTag !=null) {        
            	endpage = edPageTag.attr("content");
			}
            Element subjectTag = doc.select("meta[name*=citation_keywords]").first();
            if (subjectTag !=null) {        
            	subject = subjectTag.attr("content");
            	subject = StringHelper.cleanSemicolon(subject);
			}
            Element doiTag = doc.select("meta[name*=citation_doi]").first();
            if (doiTag !=null) {        
            	doi = doiTag.attr("content");
			}
            Element sourceTag = doc.select("meta[name*=citation_journal_title]").first();
            if (sourceTag !=null) {        
            	source = sourceTag.attr("content");
			}
            Element volumeTag = doc.select("meta[name*=citation_volume]").first();
            if (volumeTag !=null) {        
            	volume = volumeTag.attr("content");
			}
            Element issueTag = doc.select("meta[name*=citation_issue]").first();
            if (issueTag !=null) {        
            	issue = issueTag.attr("content");
			}
            Element publisherTag = doc.select("meta[name*=citation_publisher]").first();
            if (publisherTag !=null) {        
            	publisher = publisherTag.attr("content");
			}
            Element descriptionTag = doc.select("div.abstract-text > div.row > div.large-10.medium-10.small-12.columns > div > div.abstract > p").first();
            if (descriptionTag !=null) {
            	description = descriptionTag.text();
			}
            Element pissnTag = doc.select("li[class*=meta-info issn]").first();
            if (pissnTag !=null) {        
            	pissn = pissnTag.text().replace("ISSN: ", "");
            	if (pissn.length() < 8) {
            		pissn = "";
				}
			}
            Element eissnTag = doc.select("li[class*=meta-info eissn]").first();
            if (eissnTag !=null) {        
            	eissn = eissnTag.text().replace("EISSN: ", "");
            	if (eissn.length() < 8) {
            		eissn = "";
				}
			}

            Element liTag = doc.select("li[class*=author]").first();
            if (liTag !=null) {
            	for (Element authorTag: liTag.select("a")) {
            		String author = authorTag.text().replace("[", "(<(").replace("]", ")>)");
            		String supstring = "";
            		Element supTag = authorTag.nextElementSibling();
            		if (supTag != null) {
            			while (supTag.tagName().equals("sup")) {
                			supstring = supstring + supTag.text().replace("(", "").replace(")", "") + ",";
                			supTag = supTag.nextElementSibling();
                			if (supTag == null) {
								break;
							}
    					}
					}      		
                    if (supstring.length()>=1) {             
                    	author = author + "[" + supstring.substring(0,supstring.length()-1) + "]" + ";";
        			}
                    else {
                    	author = author + ";";
					}
                    creator = creator + author;
				}        
			}
            if (creator.length()>=1) {
            	creator = creator.substring(0,creator.length()-1);      
			}
            
            
            Elements institutionTags = doc.select("li[class*=contributor-affiliation]");
        	for (Element affTag: institutionTags) {
        		Element supTag = affTag.select("sup").first();
        		String supstring = "";
        		if (supTag!=null){
        			supstring = supTag.text().replace("(", "").replace(")", "");
        			supstring = "[" + supstring + "]";
        		}            		
        		Element divTag = affTag.select("div > div").first();
        		divTag.select("span").remove();
        		if (divTag.text().length()>1) {
        			insitution = insitution + supstring + divTag.text().replace(";", ",").replace("[", "(<(").replace("]", ")>)");
				}      		
        		if (insitution.length()>=1 && !insitution.endsWith(";")) {
        			insitution = insitution + ";";
				}
			}        
			
            if (insitution.length()>=1) {
            	insitution = insitution.substring(0,insitution.length()-1);      
			}
            
            String[] creator_insitution = new String[2];
			try {
				creator_insitution = AuthorOrgan.renumber(creator, insitution);
				creator = creator_insitution[0];
	            insitution = creator_insitution[1];
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            
            creator = creator.replace("(<(", "[").replace(")>)", "]");
            insitution = insitution.replace("(<(", "[").replace(")>)", "]");
            publisher = publisher.replace("&#x0027;", "'");
            
            
                      
			
            

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

			xObj.data.put("volume", volume);
			xObj.data.put("issue", issue);
			xObj.data.put("source", source);
			xObj.data.put("doi", doi);
			xObj.data.put("pissn", pissn);
			xObj.data.put("eissn", eissn);
			xObj.data.put("publisher", publisher);
			

			//String textString = String.format("%s %s %s %s %s %s %s %s %s %s %s", title,creator,class_,keyword,description,isbn,page,seriesname,charge,strreftext,rawid);
			context.getCounter("map", "count").increment(1);
			//context.write(new Text(textString), NullWritable.get());
//			if (mjid.equals("")) {
//				context.getCounter("map", "errorcount").increment(1);
//			}
			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(uid), new BytesWritable(bytes));
            
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