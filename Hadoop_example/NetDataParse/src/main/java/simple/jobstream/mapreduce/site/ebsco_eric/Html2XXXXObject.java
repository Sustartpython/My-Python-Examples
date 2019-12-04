package simple.jobstream.mapreduce.site.ebsco_eric;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

import com.google.common.base.Joiner;
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
		String jobName = "Ebsco_eric_" + this.getClass().getSimpleName();

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
			//可用性
			String availability= "";
			String page_cnt= "";
			//教育等级
			String education_Level= "";
			//出版物类型
			String type= "";
			//期刊代码
			String journal_id= "";
			//出版日期
			String date= "";
			//条目日期
			String date_2= "";
			String author_id= "";
			
			
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
							creator_value = value.replace("\\xa0", "");
							if (creator_value.contains(";")) {
								if (creator_value.split(";").length>1) {
									corr_author=creator_value.split(";")[0];
								}
							}else {
								corr_author=creator_value;
							}
							if (creator_value.contains(";")) {
								for (String cr : creator_value.split(";")) {
									String orcid_1="";
									String cr_1="";
									if (cr.length()>0) {
										if (cr.contains("ORCID")) {
											Pattern pattern = Pattern.compile("(.*?)\\((ORCID.*?)\\)");
											Matcher matcher = pattern.matcher(cr);
											if(matcher.find()){
												cr_1=matcher.group(1).trim().replaceAll("\\p{Z}", "").replaceAll("\\.$", "")+";";
												if (cr_1.equals(";")) {
													cr_1="";
												}
												orcid_1= matcher.group(2).trim().replaceAll("\\p{Z}", "")+"@"+cr_1+";";
												if (orcid_1.equals("@;")) {
													orcid_1="";
												}
											}
										} else {
											cr_1=cr.trim().replaceAll("\\p{Z}", "")+";";
											if (cr_1.equals(";")) {
												cr_1="";
											}
										}
										creator+=cr_1;
										author_id+=orcid_1.replace(";;", ";");
									}
								}
							}else {
								if (creator_value.contains("ORCID")) {
									Pattern pattern = Pattern.compile("(.*?)\\((ORCID.*?)\\)");
									Matcher matcher = pattern.matcher(creator_value);
									if(matcher.find()){
										creator=matcher.group(1).trim().replaceAll("\\p{Z}", "").replaceAll("\\.$", "")+";";
										if (creator.equals(";")) {
											creator="";
										}
										author_id= matcher.group(2).trim().replaceAll("\\p{Z}", "")+"@"+creator+";";
										if (author_id.equals("@;")) {
											author_id="";
										}
									}
								} else {
									creator=creator_value.trim();
								}
							}
							author_id=author_id.replace("ORCID", "");
						}
						if(label.equals("来源:")){
							source = value;
						}
						if(label.equals("可用性:")){
							availability = value;
						}
						if(label.equals("URL:")){
							doi=value;
							if (doi.contains("doi")) {
								doi = doi.replaceAll("^.*?10\\.", "10.").trim();	
							} else {
								doi="";
							}
							
						}
						if(label.equals("ISSN:")){
							identifier_pissn = value;
						}
						if(label.equals("描述符:")){
							subject = value.replace(",", ";");
							subject=subject+";";
						}
						if(label.equals("Location Identifiers:")){
							subject += value.replace(",", ";");
							subject=subject+";";
						}
						if(label.equals("Keyword:")){
							subject += value.replace(",", ";");
						}
						subject=subject.replaceAll(";$", "");
						String str="";
					    List list=new ArrayList();
					    Set set=new HashSet();
					    List newList = new  ArrayList();
					    String[] news = subject.split(";");//用split()函数直接分割
						for (String string : news) {
						      list.add(string);
						  	}
						for (Iterator it=list.iterator();it.hasNext();) {
					       //set能添加进去就代表不是重复的元素
					       Object element=it.next();
					       if(set.add(element)){
					           newList.add(element.toString().trim());
					       }
						}
					   	str = Joiner.on(",").join(newList);
					   	subject=str.replaceAll(",",";");
						if(label.equals("Location Identifiers:")){
							pub_place = value;
						}
						if(label.equals("摘要:")){
							description = value;
						}
						if(label.equals("参考文献数:")){
							ref_cnt = value;
							if (ref_cnt.equals("-1")) {
								ref_cnt="0";
							}
						}
						if(label.equals("语言:")){
							language = value;
						}
						if(label.equals("页数:")){
							page_cnt = value;
						}
						if(label.equals("Education Level:")){
							education_Level = value;
						}
						if(label.equals("出版物类型:")){
							type = value;
						}
						if(label.equals("期刊代码:")){
							journal_id = value;	
						}
						if(label.equals("出版日期:")){
							date = value;	
						}
						if(label.equals("条目日期:")){
							date_2 = value;	
						}
						
					}
					
					}catch(Exception e){						
						map = null;
						context.getCounter("map", "exception").increment(1);
						return map;
					}
				if(rawid.contains("ED")) {
					map = null;
					context.getCounter("map", "ED error:").increment(1);
					return map;
				}

				creator=creator.replaceAll(";$", "");
				author_id=author_id.replaceAll(";$", "");
				email=email.replaceAll("\\[[1-9]\\]", "").replaceAll("\\[[1-9],[1-9]\\]", "").replaceAll(";$", "").replaceAll("(AUTHOR)", "").trim();
				corr_author=corr_author.replaceAll("\\[[1-9]\\]", "").replaceAll("\\[[1-9],[1-9]\\]", "").replaceAll(";$", "").replaceAll("(AUTHOR)", "").trim();
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
				map.put("availability", availability);
				map.put("page_cnt", page_cnt);
				map.put("education_Level", education_Level);
				map.put("type", type);
				map.put("journal_id", journal_id);
				map.put("date", date);
				map.put("date_2", date_2);
				map.put("author_id", author_id);

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
			xObj.data.put("availability", myMap.get("availability"));
			xObj.data.put("page_cnt", myMap.get("page_cnt"));
			xObj.data.put("education_Level", myMap.get("education_Level"));
			xObj.data.put("type", myMap.get("type"));
			xObj.data.put("journal_id", myMap.get("journal_id"));
			xObj.data.put("date", myMap.get("date"));
			xObj.data.put("date_2", myMap.get("date_2"));
			xObj.data.put("author_id", myMap.get("author_id"));
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
