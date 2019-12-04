package simple.jobstream.mapreduce.site.ebsco_cmedm;

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
import java.util.LinkedHashMap;
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

import simple.jobstream.mapreduce.common.vip.AuthorOrgan;

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class Html2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 40;
	private static int reduceNum = 40;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	//这个目录会被删除重建
  
	public void pre(Job job)
	{
		String jobName = "Ebsco_cmedm_" + this.getClass().getSimpleName();

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
		private static boolean Flag;	
		
		
		public static HashMap<String,String> parseHtml(Context context, String htmlText){
			HashMap<String,String> map = new HashMap<String,String>();
			String rawid = "";
			String db = "";
			String doi="";
			String title = "";
			String all_value = "";
			String if_html_fulltext = "0";
			String if_pdf_fulltext = "0";
			String ref_cnt = "";
			String title_alt = "";
			String authors= "";
			String email= "";
			String author= "";
			String corr_author= "";
			String orange= "";
			String orange_1st= "";
			String author_1st= "";
			String source_info= "";
			String title_series= "";
			String pub_date= "";
			String pub_date_alt= "";
			String vol_info= "";
			String vol= "";
			String num= "";
			String page_info= "";
			String begin_page= "";
			String end_page= "";
			String raw_type= "";
			String language= "";
			String journal_info= "";
			String country= "";
			String journal_raw_id= "";
			String issn= "";
			String eissn= "";
			String abstract_= "";
			String index_info= "";
			String keyword= "";
			String abstract_alt= "";
			String date_info= "";
			String accept_date= "";
			String recv_date= "";
			String revision_date= "";
			String pm_id= "";
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
						if (title.length()<1) {
							context.getCounter("map", "no title:").increment(1);
							map=null;
							return map;
						}
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
						if(label.equals("音译标题:")){
							title_alt = value.trim();
						}
						
						if(label.equals("作者:")){
							authors = value;
							if (authors.contains("◆")) {
								String[] authors_list=authors.split("◆");
								String orange_all="";
								LinkedHashMap<String, String> orange_map=new LinkedHashMap<String, String>();
								for (String authors_info : authors_list) {
									String author_1="";
									String email_1="";
									if (authors_info!=null) {
										if (authors_info.contains(";")) {
											if (authors_info.contains("@")) {
												Pattern pattern = Pattern.compile("[\\w!#$%&'*+/=?^_`{|}~-]+(?:\\.[\\w!#$%&'*+/=?^_`{|}~-]+)*@(?:[\\w](?:[\\w-]*[\\w])?\\.)+[\\w](?:[\\w-]*[\\w])?");
												Matcher matcher = pattern.matcher(authors_info);
												if(matcher.find()){
													email_1 = matcher.group(0).trim();
													authors_info=authors_info.replace(email_1, "").replaceAll(",$", "").replaceAll("Electronic address:", "").trim().replaceAll("\\.$", "").trim().replaceAll("\\.$", "");}
												String[] authors_list2=authors_info.split(";");
												String orange_1="";
												if (authors_list2.length>1) {
													author_1=authors_list2[0].replaceAll(",$", "").replaceAll(".$", "").trim();
													orange_1=authors_list2[1].replaceAll("^ \\d ", "").replaceAll(",$", "").replaceAll(".$", "").trim();
													orange_all+=orange_1+";";
												}
												if (authors_list2.length>2) {
													orange_1=authors_list2[2].replaceAll("^ \\d ", "").replaceAll(",$", "").replaceAll(".$", "").trim();
													orange_all+=orange_1+";";
												}
												if (authors_list2.length>3) {
													orange_1=authors_list2[3].replaceAll("^ \\d ", "").replaceAll(",$", "").replaceAll(".$", "").trim();
													orange_all+=orange_1+";";
												}
												if (authors_list2.length>4) {
													orange_1=authors_list2[4].replaceAll("^ \\d ", "").replaceAll(",$", "").replaceAll(".$", "").trim();
													orange_all+=orange_1+";";
												}
												if (authors_list2.length>5) {
													orange_1=authors_list2[5].replaceAll("^ \\d ", "").replaceAll(",$", "").replaceAll(".$", "").trim();
													orange_all+=orange_1+";";
												}
												
											} else {
												String[] authors_list2=authors_info.split(";");
												String orange_1="";
												if (authors_list2.length>1) {
													author_1=authors_list2[0].replaceAll(",$", "").replaceAll(".$", "").trim();
													orange_1=authors_list2[1].replaceAll("^ \\d ", "").replaceAll(",$", "").replaceAll(".$", "").trim();
													orange_all+=orange_1+";";
												}
												if (authors_list2.length>2) {
													orange_1=authors_list2[2].replaceAll("^ \\d ", "").replaceAll(",$", "").replaceAll(".$", "").trim();
													orange_all+=orange_1+";";
												}
												if (authors_list2.length>3) {
													orange_1=authors_list2[3].replaceAll("^ \\d ", "").replaceAll(",$", "").replaceAll(".$", "").trim();
													orange_all+=orange_1+";";
												}
												if (authors_list2.length>4) {
													orange_1=authors_list2[4].replaceAll("^ \\d ", "").replaceAll(",$", "").replaceAll(".$", "").trim();
													orange_all+=orange_1+";";
												}
												if (authors_list2.length>5) {
													orange_1=authors_list2[5].replaceAll("^ \\d ", "").replaceAll(",$", "").replaceAll(".$", "").trim();
													orange_all+=orange_1+";";
												}
												
											}

											}
										 else {
											author_1=authors_info.trim();
										}
									}
									orange_all=orange_all.replaceAll(";$", "");
									orange_map.put(author_1, orange_all);
									orange_all="";
									if (email_1!="") {
										email+=email_1+":"+author_1+";";
										corr_author+=author_1+";";
									}
								}
							author=AuthorOrgan.numberByMap(orange_map)[0];
							orange=AuthorOrgan.numberByMap(orange_map)[1];
							email=email.replaceAll(";$", "");
							corr_author=corr_author.replaceAll(";$", "").replace("(", "").replace(")", "").trim().replaceAll("\\.$", "");
							author_1st=author.split(";")[0].replace("[1]","").replace("[2]","").replace("[3]","").replace("[1,2]","").replace("[1,3]","").replace("[2,3]","").replace("[1,2,3]","").trim();
							orange_1st=orange.split(";")[0].replace("[1]","").replace("[2]","").replace("[3]","").replace("[1,2]","").replace("[1,3]","").replace("[2,3]","").replace("[1,2,3]","").trim();
							} else {
								if (authors.contains(";")) {
									if (authors.contains("@")) {
										Pattern pattern = Pattern.compile("[\\w!#$%&'*+/=?^_`{|}~-]+(?:\\.[\\w!#$%&'*+/=?^_`{|}~-]+)*@(?:[\\w](?:[\\w-]*[\\w])?\\.)+[\\w](?:[\\w-]*[\\w])?");
										Matcher matcher = pattern.matcher(authors);
										if(matcher.find()){
											email = matcher.group(0).trim();
											authors=authors.replace(email, "").replaceAll(",$", "").replaceAll("Electronic address:", "").trim().replaceAll("\\.$", "").trim().replaceAll("\\.$", "");
											String[] authors_list=authors.split(";");
											if (authors_list.length>1) {
												author_1st=authors_list[0].replaceAll(",$", "").replaceAll("Electronic address:", "").trim().replaceAll("\\.$", "").trim().replaceAll("\\.$", "");
												orange_1st=authors_list[1].replaceAll("^ \\d ", "").replaceAll(",$", "").replaceAll("Electronic address:", "").trim().replaceAll("\\.$", "").trim().replaceAll("\\.$", "");
												orange="[1]"+orange_1st;
												author="[1]"+author_1st;
											}	
											if (authors_list.length>2) {
												orange=orange+";[2]"+authors_list[2].replaceAll("^ \\d ", "").replaceAll(",$", "").replaceAll("Electronic address:", "").trim().replaceAll("\\.$", "").trim().replaceAll("\\.$", "");
											}
											if (authors_list.length>3) {
												orange=orange+";[3]"+authors_list[3].replaceAll("^ \\d ", "").replaceAll(",$", "").replaceAll("Electronic address:", "").trim().replaceAll("\\.$", "").trim().replaceAll("\\.$", "");
											}
											if (authors_list.length>4) {
												orange=orange+";[4]"+authors_list[4].replaceAll("^ \\d ", "").replaceAll(",$", "").replaceAll("Electronic address:", "").trim().replaceAll("\\.$", "").trim().replaceAll("\\.$", "");
											}
											if (authors_list.length>5) {
												orange=orange+";[5]"+authors_list[5].replaceAll("^ \\d ", "").replaceAll(",$", "").replaceAll("Electronic address:", "").trim().replaceAll("\\.$", "").trim().replaceAll("\\.$", "");
											}
											email=email+":"+author_1st+";";
											corr_author=author_1st.trim();
									} else {
										String[] authors_list=authors.split(";");
										if (authors_list.length>1) {
											author_1st=authors_list[0].replaceAll(",$", "").replaceAll(".$", "").trim();
											orange_1st=authors_list[1].replaceAll("^ \\d ", "").replaceAll(",$", "").replaceAll(".$", "").trim();
											orange="[1]"+orange_1st;
											author="[1]"+author_1st;
										}
										if (authors_list.length>2) {
											orange=orange+";[2]"+authors_list[2].replaceAll("^ \\d ", "").replaceAll(",$", "").replaceAll("Electronic address:", "").trim().replaceAll("\\.$", "").trim().replaceAll("\\.$", "");
										}
										if (authors_list.length>3) {
											orange=orange+";[3]"+authors_list[3].replaceAll("^ \\d ", "").replaceAll(",$", "").replaceAll("Electronic address:", "").trim().replaceAll("\\.$", "").trim().replaceAll("\\.$", "");
										}
										if (authors_list.length>4) {
											orange=orange+";[4]"+authors_list[4].replaceAll("^ \\d ", "").replaceAll(",$", "").replaceAll("Electronic address:", "").trim().replaceAll("\\.$", "").trim().replaceAll("\\.$", "");
										}
										if (authors_list.length>5) {
											orange=orange+";[5]"+authors_list[5].replaceAll("^ \\d ", "").replaceAll(",$", "").replaceAll("Electronic address:", "").trim().replaceAll("\\.$", "").trim().replaceAll("\\.$", "");
										}
									}
								} else {
									author_1st=authors.trim();
									author=authors.trim();
								}
									
								}else {
									author_1st=authors.trim();
									author=authors.trim();
								}
								

							}
							
							
							
							
							
						}
						email=email.replaceAll(";$", "").replaceAll("(AUTHOR)", "").replace("(", "").replace(")", "").trim();

						if(label.equals("来源:")){
							source_info = value;
							//匹配所属丛书名称title_series
							Pattern pattern = Pattern.compile(".*?\\] ");
							Matcher matcher = pattern.matcher(source_info);
							if(matcher.find()){
								title_series=matcher.group(0).trim();
							}
							//pub_date出版日期 
							pattern = Pattern.compile("](.*?);|](.*?),");
							matcher = pattern.matcher(source_info);
							if(matcher.find()){
								pub_date=matcher.group(1).trim().replaceAll(";$", "").replaceAll("\\.$", "").replaceAll(",$", "").replace("(", "").replace(")", "");
								pub_date=pub_date.replace(title_series, "").trim().replace("(", "").replace(")", "");
							}
							//pub_date_alt  电子出版日期
							pattern = Pattern.compile("Date of Electronic Publication:.*?\\.");
							matcher = pattern.matcher(source_info);
							if(matcher.find()){
								pub_date_alt=matcher.group(0);
								pub_date_alt=pub_date_alt.replace("Date of Electronic Publication:", "").replaceAll("\\.$", "").trim();
							}
							//匹配 vol num
							pattern = Pattern.compile("Vol\\..*?,");
							matcher = pattern.matcher(source_info);
							if(matcher.find()){
								vol_info=matcher.group(0).replace("Vol.", "").replaceAll(",$", "").trim();
								vol=vol_info;
								pattern = Pattern.compile("\\d+ ");
								matcher = pattern.matcher(vol_info);
								if(matcher.find()){
									vol=matcher.group(0).trim();
									num=vol_info.replace(vol, "").trim();
									num=num.replace("(", "").replace(")", "").trim();
								}
							}
							//匹配页码 page_info   begin_page   end_page    page_cnt
							pattern = Pattern.compile("pp\\..*?;|pp\\..*?,|pp\\..*?\\.");
							matcher = pattern.matcher(source_info);
							if(matcher.find()){
								page_info=matcher.group(0).replace("pp.", "").replace("S", "").replaceAll("\\.$", "").replaceAll(";$", "").replaceAll(",$", "").trim();
								if(page_info.toLowerCase().contains("suppl")) {
									page_info ="";
								}
								int n=page_info.length()-page_info.replace("-", "").length();
								if (n==1) {
									if (page_info.split("-").length>1) {
										begin_page=page_info.split("-")[0];
										end_page=page_info.split("-")[1];
									}
								}
							}
						}	
						
						if(label.equals("出版物类型:")){
							raw_type = value.trim();
						}
						
						if(label.equals("语言:")){
							language = value.trim();
						}
						
						if(label.equals("期刊信息:")){
							journal_info = value.trim();
							Pattern pattern = Pattern.compile("Country of Publication:(.*?)NLM");
							Matcher matcher = pattern.matcher(journal_info);
							if(matcher.find()){
								country=matcher.group(1).trim();
							}
//							pattern = Pattern.compile("NLM ID:(.*?)Publication");
//							matcher = pattern.matcher(journal_info);
//							if(matcher.find()){
//								journal_raw_id=matcher.group(1).trim();
//							}
							pattern = Pattern.compile("ISSN: (.*?) ");
							matcher = pattern.matcher(journal_info);
							if(matcher.find()){
								issn=matcher.group(1).trim();
							}
							pattern = Pattern.compile("Linking ISSN:(.*?)NLM");
							matcher = pattern.matcher(journal_info);
							if(matcher.find()){
								eissn=matcher.group(1).trim();
								eissn=eissn.substring(0, 4)+"-"+eissn.substring(4,eissn.length());
							}
						}
						
						if(label.equals("摘要:")){
							abstract_ = value.trim();
						}
						
						if(label.equals("有用索引:")){
							index_info = value.trim().replace("◆", " ");
							if (index_info.contains("Keywords:") && !index_info.contains("Local Abstract:")) {
								Pattern pattern = Pattern.compile("Keywords:(.*?)$");
								Matcher matcher = pattern.matcher(index_info);
								if(matcher.find()){
									keyword=matcher.group(1).trim().replaceAll(";$", "");
								}
							}else if (!index_info.contains("Keywords:") && index_info.contains("Local Abstract:")) {
								Pattern pattern = Pattern.compile("Local Abstract:(.*?)$");
								Matcher matcher = pattern.matcher(index_info);
								if(matcher.find()){
									abstract_alt=matcher.group(1).trim().replaceAll(";$", "");
								}
							}else {
								Pattern pattern = Pattern.compile("Keywords:(.*?) Local");
								Matcher matcher = pattern.matcher(index_info);
								if(matcher.find()){
									keyword=matcher.group(1).trim();
									abstract_alt=index_info.replace(matcher.group(0).trim(), "").replace("Abstract:", "").trim().replaceAll(";$", "");
								}
							}
							Pattern pattern = Pattern.compile("DOI:(.*)$");
							Matcher matcher = pattern.matcher(abstract_alt);
							if(matcher.find()){
								doi=matcher.group(1).trim().replaceAll("\\.$", "");
								abstract_alt=abstract_alt.replace(matcher.group(0).trim(), "").trim().replaceAll(";$", "");
							}
							
						}
						
						if(label.equals("条目日期:")){
							date_info = value.trim();
							//accept_date   recv_date   revision_date
							Pattern pattern = Pattern.compile("Date Created: (\\d+) ");
							Matcher matcher = pattern.matcher(date_info);
							if(matcher.find()){
								accept_date=matcher.group(1).trim();
							}
							pattern = Pattern.compile("Date Completed: (\\d+) ");
							matcher = pattern.matcher(date_info);
							if(matcher.find()){
								recv_date=matcher.group(1).trim();
							}
							pattern = Pattern.compile("Latest Revision: (\\d+)");
							matcher = pattern.matcher(date_info);
							if(matcher.find()){
								revision_date=matcher.group(1).trim();
							}
						}
						
						if(label.equals("DOI:")){
							doi = value.trim();
						}
						
						if(label.equals("MeSH 词语:")){
							subject_word = value.trim().replace("◆", ";").replaceAll(";$", "").replaceAll("\\s+;", ";").replaceAll(";[^a-zA-Z0-9\\*-]+", ";");
						}
						
						if(label.equals("PMID:")){
							pm_id = value.trim();
						}
						if(label.equals("参考文献数:")){
							ref_cnt = value.trim();
						}
					}
					
					}catch(Exception e){						
						map = null;
						context.getCounter("map", "exception").increment(1);
						return map;
					}
				
				
				map.put("rawid", rawid);
				map.put("db", db);
				map.put("title", title);
				map.put("doi", doi);
				map.put("all_value", all_value);
				map.put("ref_cnt", ref_cnt);
				map.put("if_html_fulltext", if_html_fulltext);
				map.put("if_pdf_fulltext", if_pdf_fulltext);
				map.put("title_alt", title_alt);
				map.put("authors", authors);
				map.put("email", email);
				map.put("author", author);
				map.put("corr_author", corr_author);
				map.put("orange", orange);
				map.put("orange_1st", orange_1st);
				map.put("author_1st", author_1st);
				map.put("source_info", source_info);
				map.put("title_series", title_series);
				map.put("pub_date", pub_date);
				map.put("pub_date_alt", pub_date_alt);
				map.put("vol_info", vol_info);
				map.put("vol", vol);
				map.put("num", num);
				map.put("page_info", page_info);
				map.put("begin_page", begin_page);
				map.put("end_page", end_page);
				map.put("raw_type", raw_type);
				map.put("language", language);
				map.put("journal_info", journal_info);
				map.put("country", country);
				map.put("journal_raw_id", journal_raw_id);
				map.put("issn", issn);
				map.put("eissn", eissn);
				map.put("abstract_", abstract_);
				map.put("index_info", index_info);
				map.put("keyword", keyword);
				map.put("abstract_alt", abstract_alt);
				map.put("date_info", date_info);
				map.put("accept_date", accept_date);
				map.put("recv_date", recv_date);
				map.put("revision_date", revision_date);
				map.put("pm_id", pm_id);
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
			xObj.data.put("db", myMap.get("db"));
			xObj.data.put("doi", myMap.get("doi"));
			xObj.data.put("ref_cnt", myMap.get("ref_cnt"));
			xObj.data.put("if_html_fulltext", myMap.get("if_html_fulltext"));
			xObj.data.put("if_pdf_fulltext", myMap.get("if_pdf_fulltext"));
			xObj.data.put("title_alt", myMap.get("title_alt"));
			xObj.data.put("authors", myMap.get("authors"));
			xObj.data.put("all_value", myMap.get("all_value"));
			xObj.data.put("email", myMap.get("email"));
			xObj.data.put("corr_author", myMap.get("corr_author"));
			xObj.data.put("author", myMap.get("author"));
			xObj.data.put("orange", myMap.get("orange"));
			xObj.data.put("orange_1st", myMap.get("orange_1st"));
			xObj.data.put("author_1st", myMap.get("author_1st"));
			xObj.data.put("source_info", myMap.get("source_info"));
			xObj.data.put("title_series", myMap.get("title_series"));
			xObj.data.put("pub_date", myMap.get("pub_date"));
			xObj.data.put("pub_date_alt", myMap.get("pub_date_alt"));
			xObj.data.put("vol_info", myMap.get("vol_info"));
			xObj.data.put("vol", myMap.get("vol"));
			xObj.data.put("num", myMap.get("num"));
			xObj.data.put("page_info", myMap.get("page_info"));
			xObj.data.put("begin_page", myMap.get("begin_page"));
			xObj.data.put("end_page", myMap.get("end_page"));
			xObj.data.put("raw_type", myMap.get("raw_type"));
			xObj.data.put("language", myMap.get("language"));
			xObj.data.put("journal_info", myMap.get("journal_info"));
			xObj.data.put("country", myMap.get("country"));
			xObj.data.put("journal_raw_id", myMap.get("journal_raw_id"));
			xObj.data.put("issn", myMap.get("issn"));
			xObj.data.put("eissn", myMap.get("eissn"));
			xObj.data.put("abstract_", myMap.get("abstract_"));
			xObj.data.put("index_info", myMap.get("index_info"));
			xObj.data.put("keyword", myMap.get("keyword"));
			xObj.data.put("abstract_alt", myMap.get("abstract_alt"));
			xObj.data.put("date_info", myMap.get("date_info"));
			xObj.data.put("accept_date", myMap.get("accept_date"));
			xObj.data.put("recv_date", myMap.get("recv_date"));
			xObj.data.put("revision_date", myMap.get("revision_date"));
			xObj.data.put("pm_id", myMap.get("pm_id"));
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
