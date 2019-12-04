package simple.jobstream.mapreduce.site.aip;

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
public class Html2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 5;
	private static int reduceNum = 5;
	
	public static  String inputHdfsPath = "";
	public static  String outputHdfsPath = "";
	
	public void pre(Job job)
	{
		String jobName = "aipjournal." + this.getClass().getSimpleName();
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
		
		static int cnt = 0;	
		
		private  static Map<String, String> mapMonth =new HashMap<String, String>();
		
		public void setup(Context context) throws IOException,
			InterruptedException {
				initMapMonth();
		}
		
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
		
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	cnt += 1;
	    	if (cnt == 1) {
				System.out.println("text:" + value.toString());
			}
	    	
	    	Gson gson = new Gson();
	    	Type type = new TypeToken<Map<String, String>>(){}.getType();
	    	Map<String, String> mapJson = gson.fromJson(value.toString(), type);
	    	
	    	String url = mapJson.get("url").trim();
	    	String vol = mapJson.get("vol").trim();
	    	String issue = mapJson.get("issue").trim();
	    	String catalog = mapJson.get("catalog").trim();
	    	String doi = mapJson.get("doi").trim();
	    	String html  = mapJson.get("detail").trim();
	    	
			Document doc = Jsoup.parse(html);			
			
			String creator ="";
            String title = "";
            String insitution = "";
            String date = "";
            String year = "";
            String month = "";
            String day = "";
            String press_year="";
            String description="";
            
            Elements authorinfo = null;
            Elements institutioninfos = null;
            Element dateTag = null;
            Element descriptionTag = null;
            
            Element titleTag = doc.select("div.articleMeta > header > h1").first();
            if (titleTag !=null) {
            	title = titleTag.text();
			}else {
				titleTag = doc.select("div.hlFld-Title.articleMeta.padded-content > header > h1").first();
				if (titleTag !=null) {
	            	title = titleTag.text();
				}
				else {
					return;
				}
			}
            
            if (catalog.equals("pto")) {
            	authorinfo = doc.select("#b1 > div.entryAuthor > p > b");
            	if (authorinfo.size() == 0) {
            		authorinfo = doc.select("div.articleMeta > div.entryAuthor > a");
				}
            	for (Element info : authorinfo) {
                	creator = creator + info.text() + ";";
                }
            	
            	
            	institutioninfos = doc.select("div.articleMeta > div.affiliation");
                for (Element info : institutioninfos) {
                	info.select("a").remove();
                	insitution = insitution + info.text() + ";";
                }
                
                
                dateTag = doc.select("div.clear.date-download-container > div > span").first();
                if (dateTag != null) {
                    date = dateTag.text();
                }
                descriptionTag = doc.select("div.articleMeta > div.article-description").first();
                
			}
            else{
                authorinfo = doc.select("div.entryAuthor > span.contrib-author");
                for (Element info : authorinfo) {
                	String infonum = "";
                	Element suptag = info.select("sup").first();
                	if (suptag !=null) {
                		if (suptag.text().indexOf(")")<0) {
                			infonum = "[" + suptag.text().trim() + "]" ;
						}                		
					}                	
                	info.select("sup").remove();
                	info.select("i").remove();
                	creator = creator + infonum + info.text() + ";";
                }
                institutioninfos = doc.select("li.author-affiliation");
                for (Element info : institutioninfos) {
                	String infonum = "";
                	Element suptag = info.select("sup").first();
                	if (suptag !=null) {
                		infonum = "[" + suptag.text().trim() + "]" ;
					}                	
                	info.select("sup").remove();
                	insitution = insitution + infonum + info.text() + ";";
                }
                
                dateTag = doc.select("div.publicationContentEpubDate.dates").first();
                if (dateTag != null) {
                	date = dateTag.text().split(": ")[1];
                }
                descriptionTag = doc.select("div.hlFld-Abstract > div.NLM_paragraph").first();
            	
            }

            if (creator.length()>=1) {
            	creator = creator.substring(0,creator.length()-1);
			}
            

            if (insitution.length()>=1) {
            	insitution = insitution.substring(0,insitution.length()-1);
            	insitution = insitution.replace("(", "").replace(")", "").trim();
			}
            
            if (dateTag != null) {            
                year = date.split(" ")[2];
                month = date.split(" ")[1];
                day = date.split(" ")[0];		
			}

            
            Element pbcontentTag = doc.select("div.publicationContentCitation").first();
            if (pbcontentTag != null) {
            	Matcher pa = Pattern.compile("\\((.*?)\\)").matcher(pbcontentTag.text());
    	        if (pa.find()){
    	        	press_year = pa.group(1);
    	        }
    	        
    	        if (year.equals(press_year)) {
    				date = year + mapMonth.get(month) + day;
    			}
    	        else{
    	        	date = press_year + "0000";
    	        }				
			}                                   
            
            if (descriptionTag != null) {
                description = descriptionTag.text();
			}
			
            

			XXXXObject xObj = new XXXXObject();

			xObj.data.put("url", url);			
			xObj.data.put("vol", vol);
			xObj.data.put("issue", issue);
			xObj.data.put("catalog", catalog);
			xObj.data.put("doi", doi);
			xObj.data.put("title",title);	
			xObj.data.put("creator", creator);
			xObj.data.put("insitution", insitution);
			xObj.data.put("date", date);
			xObj.data.put("press_year", press_year);
			xObj.data.put("description", description);
			

			//String textString = String.format("%s %s %s %s %s %s %s %s %s %s %s", title,creator,class_,keyword,description,isbn,page,seriesname,charge,strreftext,rawid);
			context.getCounter("map", "count").increment(1);
			//context.write(new Text(textString), NullWritable.get());
			
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
			BytesWritable bOut = new BytesWritable();	//������������
			for (BytesWritable item : values) {
				if (item.getLength() > bOut.getLength()) {	//ѡ������һ��
					bOut.set(item.getBytes(), 0, item.getLength());
				}
			}
			
			context.getCounter("reduce", "count").increment(1);			
			
			bOut.setCapacity(bOut.getLength()); 	//��buffer��Ϊʵ�ʳ���
		
			context.write(key, bOut);
		
			
		}
	}
}