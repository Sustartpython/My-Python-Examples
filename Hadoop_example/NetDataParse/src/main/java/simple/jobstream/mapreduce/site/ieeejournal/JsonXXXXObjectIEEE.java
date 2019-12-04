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

import org.jsoup.select.Elements;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;

//将JSON格式转化为XXXXObject格式，包含去重合并
public class JsonXXXXObjectIEEE extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 40;
	private static int reduceNum = 40;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	//这个目录会被删除重建
  
	public void pre(Job job)
	{
		String jobName = "Json2XXXXObjectIEEE";
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
			//JsonObject obj = null;
			
			JsonObject obj = new JsonParser().parse(htmlText).getAsJsonObject();
			
			try{
				if(obj.has("authors")){
					JsonArray array = obj.get("authors").getAsJsonArray();
					int num = 0;
					for(JsonElement jsonElement : array){ 
					    JsonObject jo = jsonElement.getAsJsonObject();
					    String s = jo.get("name").getAsString();
					    creator = creator + s + '['+String.valueOf(num+1)+']'+';';
					    if(jo.has("affiliation")){
					    	String aff = jo.get("affiliation").getAsString();
					    	creator_institution = creator_institution+'['+String.valueOf(num+1)+']' + aff+';';
				    	}
					    num +=1;
					}
					
				}
				if(obj.has("keywords")){
					JsonArray key = obj.get("keywords").getAsJsonArray();
					for(JsonElement kElement : key){
						Gson gson = new Gson();
					    JsonObject kwdobj = kElement.getAsJsonObject();
					    String type = kwdobj.get("type").getAsString();
					    if(type.equals("IEEE Keywords")){
					    	JsonArray kwd = kwdobj.get("kwd").getAsJsonArray();
					    	for(JsonElement kwdElem : kwd){
					    		String data = kwdElem.toString();
					    		subject = subject + data+';';
					    	}
					    }
					}
				}
				subject = subject.replace("\"","" );
				if(obj.has("title")){
					title = obj.get("title").getAsString();
					}
				if(obj.has("chronOrPublicationDate")){
					date_created = obj.get("chronOrPublicationDate").getAsString();
					}
				if(obj.has("startPage")){
					startpage = obj.get("startPage").getAsString();
					}
				if(obj.has("endPage")){
					endpage = obj.get("endPage").getAsString();
						}
				if(obj.has("publisher")){
					publisher = obj.get("publisher").getAsString();
					}
				if(obj.has("issn")){
					JsonArray issn = obj.get("issn").getAsJsonArray();
					for(JsonElement jsonElement : issn){
					    JsonObject issnjo = jsonElement.getAsJsonObject();
					    if(issnjo.has("value")){
					    	String issndata = issnjo.get("value").getAsString();
					    	identifier_pissn = issndata;
				    	}
					    
					}
				}
				if(obj.has("doi")){
					doi = obj.get("doi").getAsString();
					}
				if(obj.has("abstract")){
					description = obj.get("abstract").getAsString();
				}
				if(obj.has("issue")){
					issue = obj.get("issue").getAsString();
					}
				
				if(obj.has("volume")){
					volume = obj.get("volume").getAsString();
					}
				if(obj.has("publisher")){
					publisher = obj.get("publisher").getAsString();
					}
				if(obj.has("publicationTitle")){
					source = obj.get("publicationTitle").getAsString();
				}
				if(obj.has("articleNumber")){
					rawid = obj.get("articleNumber").getAsString();
				}
				if(obj.has("publicationTitle")){
					String persistentLink = obj.get("persistentLink").getAsString();
					String[] pun = persistentLink.split("=");
					pubnum = pun[1];
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
	    	
	    	String html = text;

	    	HashMap<String, String>map = parseHtml(html);
			
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
				if (item.getLength() > bOut.getLength()) {	//ѡ����һ��
					bOut.set(item.getBytes(), 0, item.getLength());
				}
			}
			
			context.getCounter("reduce", "count").increment(1);			
			
			bOut.setCapacity(bOut.getLength()); 	//��buffer��Ϊʵ�ʳ���
		
			context.write(key, bOut);
		}
	}
}
