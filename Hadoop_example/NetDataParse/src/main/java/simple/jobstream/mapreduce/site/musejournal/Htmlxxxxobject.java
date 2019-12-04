package simple.jobstream.mapreduce.site.musejournal;

import java.io.IOException;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
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
//import org.apache.tools.ant.taskdefs.Length;
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
public class Htmlxxxxobject extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 40;
	private static int reduceNum = 40;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	//这个目录会被删除重建
  
	public void pre(Job job)
	{
		String jobName = "musejournal." + this.getClass().getSimpleName();

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
	      public static HashMap<String, String> parseHtml(String doi,String vol,String num ,String jid,String pissn,String eissn,String jname,String publishers,String issue_id,String down_date_bigjson,String htmlText) {
	            HashMap<String, String> map = new HashMap<String, String>();
	            String rawid = doi;
	            String identifier_doi = "";
	            String title = "";
	            String identifier_pissn = pissn;
	            String identifier_eissn = eissn;
	            String creator = "";
	            String source = jname;
	            String publisher = publishers;
	            String date = "";
	            String volume = vol;
	            String issue = num;
	            String description = "";
	            String page = "";
	            String date_created = "";
	            String journalId = jid;
				String down_date = down_date_bigjson;
				String parse_time = (new SimpleDateFormat("yyyyMMdd")).format(new Date());
	            if (htmlText.contains("card_text")) {

	                String text = htmlText;
	                try {
	                    Document doc = Jsoup.parse(htmlText);

	                    Element authorElement = doc.select("li[class=authors]").first();
	                    if(authorElement != null){
	                        if (authorElement.text().trim().indexOf(",") == -1){
	                        	creator =authorElement.text().trim();
	                            
	                        }
	                        else {creator =authorElement.text().trim().replace(",",";");
	                        }
	                    }

	                    Element titleElement = doc.select("li[class=title]").first();
	                    if(titleElement != null){
	                        title=titleElement.text().trim();
	                    }

	                    Element pageElement = doc.select("li[class = pg").first();
	                    if(pageElement != null){
	                        page = pageElement.text().trim().split(" ")[1];
//	                    System.out.println(page);
	                    }

	                    Element doiElement = doc.select("li[class=doi]").first();
	                    if(doiElement != null){
	                        identifier_doi=doiElement.text().trim();
	                    }

	                    Element descriptionElement = doc.select("div[class = abstract").first();
	                    if(descriptionElement != null){
	                        description = descriptionElement.text().trim();
	                    }

	                    Element dateElement  = doc.select("li[class = designation").first();
	                    if(dateElement  != null){
	                        String dates= dateElement.text().trim();
	                        Pattern pattern = Pattern.compile("(\\d{4})");
	                        Matcher matchers = pattern.matcher(dates);
	                        boolean result = matchers.find();
	                        String find_result = null;
	                        if (result) {
	                            find_result = matchers.group(1);
	                            if (Integer.valueOf(find_result) > 1700 && Integer.valueOf(find_result) < 2019) {
	                                date = find_result;
//	                                System.out.println(date);
	                            }
	                        }
	                    }

	                    Elements date_createdElement = doc.select("div.details_tbl > *");
	                    if(date_createdElement  != null){
	                        for(Element e : date_createdElement){
	                            String date_createds = e.text().trim();
	                            if(date_createds.indexOf("Launched on MUSE") != -1){
	                                date_created = date_createds.replace("Launched on MUSE ","").replace("-","");
	                            }
	                        }

	                    }


	                } catch (Exception e) {
	                    e.printStackTrace();
	                }
	                if (title.length() == 0) {
	                    return null;
	                }
	                if (rawid !=null) {
	                	map.put("rawid", rawid);
	                }else{
	                	map.put("rawid", "");
	                }
	                map.put("title", title);
	                map.put("identifier_doi", identifier_doi);
	                map.put("creator", creator);
	                map.put("date",date);
	                map.put("source", source);
	                map.put("volume", volume);
	                map.put("issue", issue);
	                map.put("identifier_pissn",identifier_pissn);
	                map.put("identifier_eissn",identifier_eissn);
	                map.put("publisher", publisher);
	                map.put("date_created", date_created);
	                map.put("description", description);
	                map.put("journalId", journalId);
	                map.put("page",page);
	                map.put("down_date", down_date);
					map.put("parse_time", parse_time);

	            } else {
	                map = null;

	            }
	            return map;

	        }
		
			
	    public void map(LongWritable key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	    	cnt += 1;
	    	if (cnt == 1) {
				System.out.println("text:" + value.toString());
			}
	    	  String text = value.toString().trim();
	    	  Gson gson = new Gson();
              Type type = new TypeToken<Map<String, Object>>() {
              }.getType();

              Map<String, Object> mapField = gson.fromJson(text, type);


              String doi = mapField.get("doi").toString();
              String article_id = mapField.get("article_id").toString();
              String vol = mapField.get("vol").toString();
              String num = mapField.get("num").toString();
              String jid = mapField.get("jid").toString();
              String pissn = mapField.get("pissn").toString();
              String eissn = mapField.get("eissn").toString();
              String jname = mapField.get("jname").toString();
              String publishers = mapField.get("publisher").toString();
              String issue_id= mapField.get("issue_id ").toString();
              
              //下次更新时，应当换成这一个
//              String down_date_bigjson = mapField.get("down_date").toString();
              String down_date_bigjson ="20181205";
              String html = mapField.get("html").toString();

//              System.out.println(issue_id);
 
              HashMap<String, String> map = new HashMap<String, String>();
              map = parseHtml(doi,vol,num ,jid,pissn,eissn,jname,publishers,issue_id,down_date_bigjson,html);
              if (map == null) {
            	  return;
              }
	    	if("".equals(map.get("rawid"))){
				context.getCounter("map", "not find rawid").increment(1);
				return;
			}

	    	if(map != null){
				XXXXObject xObj = new XXXXObject();
				xObj.data.put("rawid", map.get("rawid"));
				xObj.data.put("title", map.get("title"));
				xObj.data.put("identifier_doi", map.get("identifier_doi"));
				xObj.data.put("creator", map.get("creator"));
				xObj.data.put("date", map.get("date"));
				xObj.data.put("source", map.get("source"));
				xObj.data.put("volume", map.get("volume"));
				xObj.data.put("issue", map.get("issue"));
				xObj.data.put("identifier_pissn", map.get("identifier_pissn"));
				xObj.data.put("identifier_eissn", map.get("identifier_eissn"));
				xObj.data.put("publisher", map.get("publisher"));
				xObj.data.put("date_created", map.get("date_created"));
				xObj.data.put("description", map.get("description"));
				xObj.data.put("journalId", map.get("journalId"));		
				xObj.data.put("page", map.get("page"));
				xObj.data.put("down_date", map.get("down_date"));
				xObj.data.put("parse_time", map.get("parse_time"));
							
				byte[] bytes = com.process.frame.util.VipcloudUtil.SerializeObject(xObj);
				context.write(new Text(map.get("rawid")), new BytesWritable(bytes));	
				context.getCounter("map", "count").increment(1);
	    	}else{
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
