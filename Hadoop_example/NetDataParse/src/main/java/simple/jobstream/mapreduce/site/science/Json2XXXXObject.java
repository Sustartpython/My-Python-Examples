package simple.jobstream.mapreduce.site.science;

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

//统计wos和ei的数据量
public class Json2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 5;
	private static int reduceNum = 5;
	
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
	    	String vol = mapJson.get("vol").trim();
	    	String issue = mapJson.get("issue").trim();
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
//            String mjid="";
            
            Element titleTag = doc.select("meta[name*=citation_title]").first();
            if (titleTag !=null) {        
            	title = titleTag.attr("content");
			}
            Element dateTag = doc.select("meta[name*=citation_publication_date]").first();
            if (dateTag !=null) {        
            	date_created = dateTag.attr("content").replace("/", "");
			}
            Element bgPageTag = doc.select("meta[name*=citation_firstpage]").first();
            if (bgPageTag !=null) {        
            	beginpage = bgPageTag.attr("content");
			}
            Element edPageTag = doc.select("meta[name*=citation_lastpage]").first();
            if (edPageTag !=null) {        
            	endpage = edPageTag.attr("content");
			}
            Element subjectTag = doc.select("meta[name*=citation_article_type]").first();
            if (subjectTag !=null) {        
            	provider_subject = subjectTag.attr("content");
			}
            Element doiTag = doc.select("meta[name*=citation_doi]").first();
            if (doiTag !=null) {        
            	doi = doiTag.attr("content");
			}
            Element sourceTag = doc.select("meta[name*=citation_journal_title]").first();
            if (sourceTag !=null) {        
            	source = sourceTag.attr("content");
			}
//            Element mjidTag = doc.select("meta[name*=citation_mjid]").first();
//            if (mjidTag !=null) {        
//            	mjid = mjidTag.attr("content");
//            	mjid = mjid.replace(";", "/");
//			}
            
            
            Element descriptionTag = doc.select("div[class*=section abstract]").first();
            if (descriptionTag !=null) {
            	descriptionTag.select("h2").remove();
            	description = descriptionTag.text();
			}
            else {
            	descriptionTag = doc.select("div[class*=section summary]").first();
            	if (descriptionTag !=null) {
            		descriptionTag.select("h2").remove();
                	description = descriptionTag.text();
    			}
			}
            Element olTag = doc.select("ol[class*=contributor-list]").first();
            if (olTag !=null) {
            	for (Element authorTag: olTag.select("li")) {
            		String author = authorTag.select("span").first().text();
            		String supstring = "";
            		for (Element supTag: authorTag.select("sup")) {
            			if (isNumeric(supTag.text())) {
            				supstring = supstring + supTag.text() + ",";
						}            			
					}
            		String asup = "";
            		for (Element asupTag: authorTag.select("a")) {
            			if (isNumeric(asupTag.text())) {
            				asup = asup + asupTag.text() + ",";
						}              			
					}
            		
                    if (supstring.length()>=1) {
                    	supstring = supstring.substring(0,supstring.length()-1);
                    	author = author + "[" + supstring + "]" + ";";
        			}
                    else if (asup.length()>=1) {
                    	asup = asup.substring(0,asup.length()-1);
                    	author = author + "[" + asup + "]" + ";";
					}
                    else {
                    	author = author + ";";
					}
                    creator = creator + author;
				}        
			}
            if (creator.equals("")) {
            	 Element spanTag = doc.select("span[class*=highwire-citation-authors]").first();
                 if (spanTag !=null) {
                 	for (Element authorTag: spanTag.select("span")) {
                 		String author = authorTag.text();
                 		if (!author.equals("")) {
                 			creator = creator + author + ";";
						}
                 		
                 	}
                 }
			}
            if (creator.length()>=1) {
            	creator = creator.substring(0,creator.length()-1);      
			}
            
            
            Element institutionTag = doc.select("ol[class*=affiliation-list]").first();
            if (institutionTag !=null) {
            	for (Element affTag: institutionTag.select("address")) {
            		Element supTag = affTag.select("sup").first();
            		String supstring = "";
            		if (supTag!=null){
            			supstring = supTag.text();
            			supstring = "[" + supstring + "]";
            		}            		
            		
            		affTag.select("sup").remove();
            		insitution = insitution + supstring + affTag.text() + ";";
				}        
			}
            if (insitution.length()>=1) {
            	insitution = insitution.substring(0,insitution.length()-1);      
			}
            
            if (description.equals("")) {
            	descriptionTag = doc.select("div[class*=article fulltext-view]").first();
                if (descriptionTag !=null) {
                	description = descriptionTag.text();
    			}
			}
            
            if (provider_subject.equals("")) {
            	Element divTag = doc.select("div[class*=compilation]").first();
                if (divTag !=null) {
                	description = divTag.text();
                	creator = "";
    			}
			}
            
            if (source.equals("Sci. Signal.")) {
            	source = "Science Signaling";
			}
            else if (source.equals("Sci. STKE")) {
            	source = "Science Signaling";
			}
            
            String rawid = url.replace(".sciencemag.org/content/", "_");
//            rawid = rawid.replace("/", "_");
            title = title.replaceAll("\\s+", " ");
            
            
            
           
			
            

			XXXXObject xObj = new XXXXObject();


			xObj.data.put("title",title);	
			xObj.data.put("creator", creator);
			xObj.data.put("beginpage", beginpage);
			xObj.data.put("endpage", endpage);
			xObj.data.put("date_created", date_created);			
			xObj.data.put("provider_subject", provider_subject);
			xObj.data.put("insitution", insitution);
			xObj.data.put("description", description);
			xObj.data.put("url", url);
			xObj.data.put("vol", vol);
			xObj.data.put("issue", issue);
			xObj.data.put("source", source);
			xObj.data.put("doi", doi);
			

			//String textString = String.format("%s %s %s %s %s %s %s %s %s %s %s", title,creator,class_,keyword,description,isbn,page,seriesname,charge,strreftext,rawid);
			context.getCounter("map", "count").increment(1);
			//context.write(new Text(textString), NullWritable.get());
//			if (mjid.equals("")) {
//				context.getCounter("map", "errorcount").increment(1);
//			}
			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(rawid), new BytesWritable(bytes));
            
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