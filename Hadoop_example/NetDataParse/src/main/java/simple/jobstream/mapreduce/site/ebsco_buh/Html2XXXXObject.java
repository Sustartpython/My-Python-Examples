package simple.jobstream.mapreduce.site.ebsco_buh;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
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
public class Html2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 40;
	private static int reduceNum = 40;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	//这个目录会被删除重建
  
	public void pre(Job job)
	{
		String jobName = "Ebsco_buh_" + this.getClass().getSimpleName();

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
		//记录日志到HDFS
		public boolean log2HDFSForMapper(Context context, String text) {
			Date dt=new Date();//如果不需要格式,可直接用dt,dt就是当前系统时间
			DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");//设置显示格式
			String nowTime = df.format(dt);//用DateFormat的format()方法在dt中获取并以yyyy/MM/dd HH:mm:ss格式显示
			
			df = new SimpleDateFormat("yyyyMMdd");//设置显示格式
			String nowDate = df.format(dt);//用DateFormat的format()方法在dt中获取并以yyyy/MM/dd HH:mm:ss格式显示
			
			text = nowTime + "\n" + text + "\n\n";
			
			boolean bException = false;
			BufferedWriter out = null;
			try {
				// 获取HDFS文件系统  
		        FileSystem fs = FileSystem.get(context.getConfiguration());
		  
		        FSDataOutputStream fout = null;
		        String pathfile = "/user/qianjun/log/log_map/" + nowDate + ".txt";
		        // 判断是否存在日志目录
		        if (fs.exists(new Path(pathfile))) {
		        	fout = fs.append(new Path(pathfile));
				}
		        else {
		        	fout = fs.create(new Path(pathfile));
		        }
		        
		        out = new BufferedWriter(new OutputStreamWriter(fout, "UTF-8"));
			    out.write(text);
			    out.close();
			    
			} catch (Exception ex) {
				bException = true;
			}
			
			if (bException) {
				return false;
			}
			else {
				return true;
			}
		}
		static int cnt = 0;	
		
		
		public static HashMap<String,String> parseHtml(Context context, String htmlText){
			HashMap<String,String> map = new HashMap<String,String>();
			String rawid = "";
			String db = "";
			String doi="";
			String title = "";
			String title_alternative = "";
			String identifier_pissn="";
			String creator="";
			String creator_institution = "";
			String source = "";
			String description = "";
			String description_en = "";
			String subject ="";
			String language = "";
			String raw_type = "";
			String creator_value= "";
			String corr_author= "";
			String email= "";
			String if_html_fulltext= "0";
			String if_pdf_fulltext= "0";
			String ref_cnt= "";
			String pub_place= "";
			String all_value= "";
			String subject_word= "";
			
			
			if(htmlText.contains("★")){
				rawid = htmlText.split("★")[0].toString();
				db = htmlText.split("★")[1].toString();
				
				String text = htmlText.split("★")[2].toString().replace("<sup>", "[").replace("</sup>", "]").replace("<br />", "◆");
//				String text = htmlText.split("★")[2].toString().replace("\\n", "");
				Document doc = Jsoup.parse(text.toString());
				// if_html_fulltext    if_pdf_fulltext
				all_value=doc.select("div#Column1Content").text().trim();
				if (all_value.contains("HTML 全文")) {
					if_html_fulltext="1";
				}
				if (all_value.contains("PDF 全文")) {
					if_pdf_fulltext="1";
				}
				if (all_value.contains("参考文献")) {
					Pattern pattern = Pattern.compile("参考文献 \\(\\d*\\)");
					Matcher matcher = pattern.matcher(all_value);
					if(matcher.find()){
						ref_cnt = matcher.group(0).trim().replace("参考文献 (", "").replace(")", "");				
					}
				}
				
				
				try{
					// 取出书名
					if(doc.select("h1[class = hidden]").first()!=null){
						title = doc.select("h1[class = hidden]").first().attr("title").trim();
						
					}
					//类别：标题、来源、类型、公司实体、行业代码、摘要、编号等
					List<Element> citation_field_label = doc.select("dt[data-auto = citation_field_label]");
					//结果：标题、来源、类型、公司实体、行业代码、摘要、编号等
					List<Element> citation_field_value = doc.select("dd[data-auto = citation_field_value]");
					
					String label = "";
					String value= "";
					for(int i=0; i<citation_field_label.size(); i++){
						label = citation_field_label.get(i).text().trim();
						value = citation_field_value.get(i).text().trim();
						//creator corr_author  email
						if(label.equals("作者:")){
							creator_value = value;
							if (creator_value.contains("◆")) {
								String[] creator_list=creator_value.split("◆");
								for (String string1 : creator_list) {
									if (string1!=null) {
										String email_1="";
										String creator_1="";
										String corr_author_1="";
										if (string1.contains("@")) {
											Pattern pattern = Pattern.compile("[\\w!#$%&'*+/=?^_`{|}~-]+(?:\\.[\\w!#$%&'*+/=?^_`{|}~-]+)*@(?:[\\w](?:[\\w-]*[\\w])?\\.)+[\\w](?:[\\w-]*[\\w])?");
											Matcher matcher = pattern.matcher(string1);
											if(matcher.find()){
												email_1 = matcher.group(0).trim();
												creator_1=string1.replaceAll(email_1, "").trim();
												corr_author_1=creator_1.trim();
												email += email_1+":"+creator_1+";";
												creator +=creator_1+";";
												corr_author += corr_author_1+";";
											}
										} else {
											creator_1=string1.trim();
											creator +=creator_1+";";
										}
									}
								}
							} else {
								
								if (creator_value.contains("@")) {
									Pattern pattern = Pattern.compile("[\\w!#$%&'*+/=?^_`{|}~-]+(?:\\.[\\w!#$%&'*+/=?^_`{|}~-]+)*@(?:[\\w](?:[\\w-]*[\\w])?\\.)+[\\w](?:[\\w-]*[\\w])?");
									Matcher matcher = pattern.matcher(creator_value);
									if(matcher.find()){
										email = matcher.group(0).trim();
										creator=creator_value.replaceAll(email, "").trim();
										email=email+":"+creator;
										corr_author=creator.trim();
									}
								} else {
									creator=creator_value.trim();
								}
							}
						}
						if(label.equals("替代标题:")){
							title_alternative = value;
						}
						if(label.equals("地理术语:")){
							pub_place = value;
						}
						if(label.equals("语言:")){
							language = value;
						}
						if(label.equals("文献类型:")){
							raw_type = value;
						}
						
						if(label.equals("DOI:")){
							doi = value;	
						}
						
						if(label.equals("作者单位:")){
							creator_institution = value;	
						}
						
						if(label.equals("来源:")){
							source = value;
							
						}
						if(label.equals("作者提供的关键字:")){
							subject = value;
						
						}
						if(label.equals("主题语:")){
							if(subject == ""){
								subject_word = value.replace("*","");
							}
							
						
						}
						
						if(label.equals("ISSN:")){
							identifier_pissn = value;
							
						}
						if(label.equals("摘要:")){
							description = value;
							
						}

						if(label.equals("摘要（英语）:")){
							if (description ==""){
								description = value;
							}else{
								description_en = value;
							}
							
							
						}
						if(label.equals("摘要（西班牙语）:") || label.equals("摘要（法语）:") || label.equals("摘要（德语）:")|| label.contains("Abstract")){
							if (description ==""){
								description = value;
							}else if (description_en ==""){
								description_en = value;
							}
							
							continue;
							
						}
						
					}
					
					}catch(Exception e){						
						map = null;
						context.getCounter("map", "exception").increment(1);
						return map;
					}
				
//				if(rawid == "" || db == ""){
//					map = null;
//				}
				
				
				
				
				email=email.replaceAll("\\[[1-9]\\]", "").replaceAll("\\[[1-9],[1-9]\\]", "").replaceAll(";$", "").replaceAll("(AUTHOR)", "").replace("(", "").replace(")", "").trim();
				corr_author=corr_author.replaceAll("\\[[1-9]\\]", "").replaceAll("\\[[1-9],[1-9]\\]", "").replaceAll(";$", "").replaceAll("(AUTHOR)", "").replace("(", "").replace(")", "").trim().replaceAll("\\.$", "");
				map.put("rawid", rawid);
				map.put("db", db);
				map.put("title", title);
				map.put("title_alternative", title_alternative);
				map.put("language", language);
				map.put("creator_institution", creator_institution);
				map.put("identifier_pissn", identifier_pissn);
				map.put("creator", creator);
				map.put("source", source);
				map.put("description", description);
				map.put("description_en", description_en);
				map.put("subject", subject);
				map.put("doi", doi);
				map.put("raw_type", raw_type);
				map.put("corr_author", corr_author);
				map.put("email", email);
				map.put("creator_value", creator_value);
				map.put("pub_place", pub_place);
				map.put("all_value", all_value);
				map.put("ref_cnt", ref_cnt);
				map.put("if_html_fulltext", if_html_fulltext);
				map.put("if_pdf_fulltext", if_pdf_fulltext);
				map.put("subject_word", subject_word);

			}else{
				context.getCounter("map", "no ★").increment(1);
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
	    	HashMap<String,String> myMap = parseHtml(context, value.toString());
	    	if (myMap == null){
	    		log2HDFSForMapper(context, "##" + value.toString() + "**");
	    		context.getCounter("map", "null").increment(1);
	    		return;
	    	}
	    	String db = myMap.get("db");

			context.getCounter("map", db).increment(1);
	    	
			XXXXObject xObj = new XXXXObject();
			xObj.data.put("rawid", myMap.get("rawid"));
			xObj.data.put("title", myMap.get("title"));
			xObj.data.put("title_alternative", myMap.get("title_alternative"));
			xObj.data.put("language", myMap.get("language"));
			xObj.data.put("db", myMap.get("db"));
			xObj.data.put("doi", myMap.get("doi"));
			xObj.data.put("creator_institution", myMap.get("creator_institution"));
			xObj.data.put("identifier_pissn", myMap.get("identifier_pissn"));
			xObj.data.put("creator", myMap.get("creator"));
			xObj.data.put("source_all", myMap.get("source"));
			xObj.data.put("description", myMap.get("description"));
			xObj.data.put("description_en", myMap.get("description_en"));
			xObj.data.put("subject", myMap.get("subject"));
			xObj.data.put("raw_type", myMap.get("raw_type"));
			xObj.data.put("corr_author", myMap.get("corr_author"));
			xObj.data.put("email", myMap.get("email"));
			xObj.data.put("creator_value", myMap.get("creator_value"));
			xObj.data.put("pub_place", myMap.get("pub_place"));
			xObj.data.put("all_value", myMap.get("all_value"));
			xObj.data.put("ref_cnt", myMap.get("ref_cnt"));
			xObj.data.put("if_html_fulltext", myMap.get("if_html_fulltext"));
			xObj.data.put("if_pdf_fulltext", myMap.get("if_pdf_fulltext"));
			xObj.data.put("subject_word", myMap.get("subject_word"));
			byte[] bytes = com.process.frame.util.VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(myMap.get("db") + "_" + myMap.get("rawid")  ), new BytesWritable(bytes));			
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
