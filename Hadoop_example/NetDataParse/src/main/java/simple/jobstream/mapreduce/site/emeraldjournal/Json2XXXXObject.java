package simple.jobstream.mapreduce.site.emeraldjournal;

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
	private static int reduceNum = 20;
	
	public static  String inputHdfsPath = "";
	public static  String outputHdfsPath = "";
	
	public void pre(Job job)
	{
		String jobName = "emeraldjournal." + this.getClass().getSimpleName();
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
		private  static Map<String, String> mapMonth =new HashMap<String, String>();
		
		private static void initMapMonth() {
			mapMonth.put("January", "01");
			mapMonth.put("February", "02");
			mapMonth.put("March", "03");
			mapMonth.put("April", "04");
			mapMonth.put("May", "05");
			mapMonth.put("June", "06");
			mapMonth.put("July", "07");
			mapMonth.put("August", "08");
			mapMonth.put("September", "09");
			mapMonth.put("October", "10");
			mapMonth.put("November", "11");
			mapMonth.put("December", "12");
		}
		
		
		public void setup(Context context) throws IOException,
			InterruptedException {
			initMapMonth();
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
	    	String volume = mapJson.get("vol").trim();
	    	String issue = mapJson.get("issue").trim();
	    	String gch = mapJson.get("gch").trim();
	    	String html  = mapJson.get("detail").trim();
	    	
			Document doc = Jsoup.parse(html);						
            
            String creator ="";
            String title = "";
            String date_created = "";
            String date = "";
            String provider_subject="";
            String description="";
            String doi="";
            String beginpage="";
            String endpage="";
            String insitution="";
            String source="";
            String subject="";
            String pissn="";
            String publisher="";

            
            Element titleTag = doc.select("article > h1 > span").first();
            
            if (titleTag !=null) {        
            	title = titleTag.text();
			}

//            Element doiTag = doc.select("meta[scheme*=doi]").first();
//            if (doiTag !=null) {        
//            	doi = doiTag.attr("content");
//			}
            doi = url.replace("/doi/abs/", "");
            Element sourceTag = doc.select("meta[name*=citation_journal_title]").first();
            if (sourceTag !=null) {        
            	source = sourceTag.attr("content");
			}
            Element subjectTag = doc.select("meta[name*=keywords]").first();
            if (subjectTag !=null) {        
            	subject = subjectTag.attr("content").replace(", ", ";").replace(",", ";");
			}
            Element publisherTag = doc.select("meta[name*=dc.Publisher]").first();
            if (publisherTag !=null) {        
            	publisher = publisherTag.attr("content").trim();
			}
            Element issnTag = doc.select("meta[name*=DCSext.WT_issn]").first();
            if (issnTag !=null) {        
            	pissn = issnTag.attr("content");
			}
            
            
            Element descriptionTag = doc.select("article > dl > dd.abstract > span").first();
            if (descriptionTag !=null) {
            	description = descriptionTag.text();
			}
            
            LinkedHashMap<String, String> mapcre_ins = new LinkedHashMap<String, String>();
            Pattern ptCre_ins = Pattern.compile("\\((.*)\\)");
            
            Element authorTag = doc.select("article > dl > dd > div.authors").first();

            if (authorTag !=null){
            	String cre = authorTag.select("a > span").text().trim();
            	authorTag.select("a > span").remove();
            	String strins = authorTag.text().trim();
            	Matcher matcher = ptCre_ins.matcher(strins);
            	String ins = "";
            	while (matcher.find()) {
            		String[] strmatch = matcher.group(1).split("\\)\\s*\\(");
            		for (int i = 0; i < strmatch.length; i++) {
            			ins = ins + strmatch[i].replaceAll("^,\\s*", "").replace(";", ",") + ";";
					}        		
				}
            	if (ins.length()>0) {
            		ins = ins.substring(0, ins.length()-1);
				}
            	mapcre_ins.put(cre, ins);
            }
            else {
				for (Element exauthorTag : doc.select("div.contentContribs")){
					String cre = exauthorTag.select("a > span").text().trim();
					exauthorTag.select("a > span").remove();
					String strins = exauthorTag.text().trim();
	            	Matcher matcher = ptCre_ins.matcher(strins);
	            	String ins = "";
	            	while (matcher.find()) {
	            		String[] strmatch = matcher.group(1).split("\\)\\s*\\(");
	            		for (int i = 0; i < strmatch.length; i++) {
	            			ins = ins + strmatch[i].replaceAll("^,\\s*", "").replace(";", ",") + ";";
						}  
					}
	            	if (ins.length()>0) {
	            		ins = ins.substring(0, ins.length()-1);
					}
	            	mapcre_ins.put(cre, ins);
				}
			}
            
            for (Element exauthorTag : doc.select("article > dl > dd > div.expandable-author > div")){
            	String cre = exauthorTag.select("a > span").text().trim();
            	exauthorTag.select("a > span").remove();
            	String strins = exauthorTag.text().trim();
            	Matcher matcher = ptCre_ins.matcher(strins);
            	String ins = "";
            	while (matcher.find()) {
            		String[] strmatch = matcher.group(1).split("\\)\\s*\\(");
            		for (int i = 0; i < strmatch.length; i++) {
            			ins = ins + strmatch[i].replaceAll("^,\\s*", "").replace(";", ",") + ";";
					}  
				}
            	if (ins.length()>0) {
            		ins = ins.substring(0, ins.length()-1);
				}
            	mapcre_ins.put(cre, ins);

            }
            
            String[] result = AuthorOrgan.numberByMap(mapcre_ins);
            creator = result[0];
            insitution = result[1];
            
            for (Element dtTag : doc.select("article > dl > dt")){
            	if (dtTag.text().startsWith("Accepted")){
            		String datetemp = dtTag.nextElementSibling().text().trim();
            		String[] datetime = datetemp.split(" ");
            		if (mapMonth.get(datetime[1]) != null) {
            			date_created = datetime[2] + mapMonth.get(datetime[1]) + datetime[0];
					}    		
            	}
            	else if (dtTag.text().startsWith("Type")) {
					provider_subject = dtTag.nextElementSibling().text().trim();
				}
            	else if (dtTag.text().startsWith("Citation")) {
					String citation = dtTag.nextElementSibling().text().trim();
					Matcher pagenum = Pattern.compile("pp\\.(\\d+)-(\\d+)").matcher(citation);
					if (pagenum.find()) {
						beginpage = pagenum.group(1);
						endpage = pagenum.group(2);
					}
					Matcher datetime = Pattern.compile("\\((\\d{4})\\)").matcher(citation);
					if (datetime.find()) {
						date = datetime.group(1);
					}
					if (date_created.equals("") && !date.equals("")) {
						date_created = date + "0000";					
					}
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
			xObj.data.put("date", date);
			xObj.data.put("subject", subject);
			xObj.data.put("insitution", insitution);
			xObj.data.put("description", description);
			xObj.data.put("provider_subject", provider_subject);
			xObj.data.put("volume", volume);
			xObj.data.put("issue", issue);
			xObj.data.put("source", source);
			xObj.data.put("doi", doi);
			xObj.data.put("pissn", pissn);
			xObj.data.put("publisher", publisher);
			

			//String textString = String.format("%s %s %s %s %s %s %s %s %s %s %s", title,creator,class_,keyword,description,isbn,page,seriesname,charge,strreftext,rawid);
			context.getCounter("map", "count").increment(1);
			//context.write(new Text(textString), NullWritable.get());
//			if (mjid.equals("")) {
//				context.getCounter("map", "errorcount").increment(1);
//			}
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