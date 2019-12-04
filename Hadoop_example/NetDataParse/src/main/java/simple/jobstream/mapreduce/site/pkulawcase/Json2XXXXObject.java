package simple.jobstream.mapreduce.site.pkulawcase;

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
import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;
import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.util.StringHelper;

//统计wos和ei的数据量
public class Json2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 5;
	private static int reduceNum = 100;
	
	public static  String inputHdfsPath = "";
	public static  String outputHdfsPath = "";
	
	
	public void pre(Job job)
	{
		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		  outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
		  reduceNum = Integer.parseInt(job.getConfiguration().get("reduceNum"));
		  job.setJobName(job.getConfiguration().get("jobName"));
		  
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
		job.setReducerClass(UniqXXXXObjectReducer.class);

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
		public  String batch="";
		
		public void setup(Context context) throws IOException,
			InterruptedException {
			batch = context.getConfiguration().get("batch");
		}
		
		static String cleanSpace(String text) {
			text = text.replaceAll("[\\s\\p{Zs}]+", " ").trim();		
			return text;
		}
		
		
		
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	
	    	Gson gson = new Gson();
	    	Type type = new TypeToken<Map<String, String>>(){}.getType();
	    	Map<String, String> mapJson =null;
	    	try {
	    		mapJson = gson.fromJson(value.toString(), type);
			} catch (Exception e) {
				context.getCounter("map", "error line").increment(1);
				return;
				// TODO: handle exception
			}
	    	
	    	
	    	String db = mapJson.get("db").trim();
	    	String gid = mapJson.get("gid").trim();
	    	String html  = mapJson.get("html").trim();
	    	
			Document doc = Jsoup.parse(html);						
            
			String creator ="";
            String title = "";
            String date_created = "";
            String subject="";
            String description="";
            String creator_release="";
            String identifier_standard="";
            String date_impl="";
            String description_type="";
            String subject_dsa="";
            String contributor="";
            String legal_status="";
            String provider_subject="";
            String agents="";
            String agency="";
            String title_edition="";
            String source="";


            Element titleTag = doc.select("#fulltextmain > div.detailLeft > div > h3").first();
            if (titleTag !=null) {
            	titleTag.select("span").remove();
            	title = titleTag.text();
			}
            if (title.contains("***")) {
            	context.getCounter("map", "error title").increment(1);
            	return;
			}
			Element table = doc.select("#fulltextmain > div.detailLeft > div > table").first();
			if (table == null) {
				context.getCounter("map", "null table id").increment(1);
				return;
			}
			Element descriptionTag = doc.select("#divFullText").first();
			if (descriptionTag !=null) {
				description = descriptionTag.text();
			}
			
					
			for (Element element : table.select("td")) {
				if (element.text().trim().startsWith("【审理法院】")||element.text().trim().startsWith("【裁决机构】")) {
					creator_release = element.nextElementSibling().text().trim();
					creator_release = cleanSpace(creator_release).replace("，", ";");
				}
				else if (element.text().trim().startsWith("【案由】")||element.text().trim().startsWith("【参照级别】")) {
					for (Element aTag : element.nextElementSibling().select("a")) {
						subject_dsa = subject_dsa + aTag.text() + ";";
					}				
					subject_dsa = StringHelper.cleanSemicolon(subject_dsa);
				}
				else if (element.text().trim().startsWith("【案件字号】")) {
					identifier_standard = element.nextElementSibling().text().trim();
					identifier_standard = cleanSpace(identifier_standard);
				}
				else if (element.text().trim().startsWith("【审理法官】")) {
					contributor = element.nextElementSibling().text().trim();
					contributor = cleanSpace(contributor).replace(" ", ";");
				}
				else if (element.text().trim().startsWith("【文书类型】")) {
					provider_subject = element.nextElementSibling().text().trim();
					provider_subject = cleanSpace(provider_subject).replace(" ", ";");
				}
				else if (element.text().trim().startsWith("【审结日期】")||element.text().trim().startsWith("【裁决日期】")) {
					date_impl = element.nextElementSibling().text().trim();
					date_impl = cleanSpace(date_impl).replace(".", "");
				}
				else if (element.text().trim().startsWith("【代理律师/律所】")) {
					String agen = element.nextElementSibling().text().trim();
					agen = cleanSpace(agen);
					for (String agstring : agen.split("；")) {
						if (agstring.contains("，")) {
							agents = agents + agstring.split("，")[0]+";";
							if (!agstring.endsWith("，")) {
								agency = agency + agstring.split("，")[1]+";";
							}							
						}
					}
					if (agents.length()>0) {
						agents = agents.substring(0,agents.length()-1);
						agents = StringHelper.cleanSemicolon(agents);
					}
					if (agency.length()>0) {
						agency = agency.substring(0,agency.length()-1);
						agency = cleanSpace(agency);
					}
				}
				else if (element.text().trim().startsWith("【权责关键词】")) {
					subject = element.nextElementSibling().text().trim();
					subject = cleanSpace(subject).replace(" ", ";");
				}
				else if (element.text().trim().startsWith("【发布日期】")) {
					date_created = element.nextElementSibling().text().trim();
					date_created = cleanSpace(date_created).replace("/", "").replace(".", "");
				}
				else if (element.text().trim().startsWith("【审理程序】")) {
					title_edition = element.nextElementSibling().text().trim();
					title_edition = cleanSpace(title_edition);
				}
				else if (element.text().trim().startsWith("【来源】")) {
					source = element.nextElementSibling().text().trim();
					source = cleanSpace(source);
				}
			}
            
            
            
                      
			

			XXXXObject xObj = new XXXXObject();

			xObj.data.put("db", db);
			xObj.data.put("gid", gid);
			xObj.data.put("title",title);	
			xObj.data.put("description", description);
			xObj.data.put("creator_release", creator_release);
			xObj.data.put("subject_dsa", subject_dsa);
			xObj.data.put("identifier_standard", identifier_standard);						
			xObj.data.put("subject", subject);		
			xObj.data.put("contributor", contributor);
			xObj.data.put("provider_subject", provider_subject);
			xObj.data.put("date_impl", date_impl);
			xObj.data.put("agents", agents);
			xObj.data.put("agency", agency);
			xObj.data.put("subject", subject);
			xObj.data.put("date_created", date_created);
			xObj.data.put("title_edition", title_edition);
			xObj.data.put("source", source);
			xObj.data.put("batch", batch);

			
			

			context.getCounter("map", "count").increment(1);

			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(db+"_"+gid), new BytesWritable(bytes));
            
	    }

	}
}