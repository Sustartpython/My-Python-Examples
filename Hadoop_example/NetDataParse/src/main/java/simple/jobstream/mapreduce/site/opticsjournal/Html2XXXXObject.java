package simple.jobstream.mapreduce.site.opticsjournal;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.time.Year;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FSDataInputStream;
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
import org.hamcrest.core.Is;
import org.json.JSONObject;
//import org.apache.tools.ant.taskdefs.Length;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.junit.Test;

import com.cloudera.io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import com.cloudera.org.codehaus.jackson.node.TextNode;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.healthmarketscience.jackcess.util.OleBlob.Content;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.BXXXXObject;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.JobConfUtil;
import com.process.frame.util.SimpleTextInputFormat;
import com.process.frame.util.VipcloudUtil;
import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.AuthorOrgan;
import simple.jobstream.mapreduce.common.vip.LogMR;
import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;

//import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class Html2XXXXObject extends InHdfsOutHdfsJobInfo {

	private static int reduceNum = 0;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = ""; // 这个目录会被删除重建

	public void pre(Job job) {
		job.setJobName(job.getConfiguration().get("jobName"));
		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
		reduceNum = Integer.parseInt(job.getConfiguration().get("reduceNum"));
	}

	public String getHdfsInput() {
		return inputHdfsPath;
	}

	public String getHdfsOutput() {
		return outputHdfsPath;
	}

	public void SetMRInfo(Job job) {
//		job.getConfiguration().set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
//		System.out.println(job.getConfiguration().get("io.compression.codecs"));
		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.7f);
		System.out.println("******mapred.reduce.slowstart.completed.maps*******"
				+ job.getConfiguration().get("mapred.reduce.slowstart.completed.maps"));
		job.getConfiguration().set("io.compression.codecs",
				"org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(UniqXXXXObjectReducer.class);
//		job.setReducerClass(ProcessReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		SequenceFileOutputFormat.setCompressOutput(job, false);
		job.setNumReduceTasks(reduceNum);
	}

	public void post(Job job) {

	}

	public String GetHdfsInputPath() {
		return inputHdfsPath;
	}

	public String GetHdfsOutputPath() {
		return outputHdfsPath;
	}
	

	public static class ProcessMapper extends Mapper<LongWritable, Text, Text, BytesWritable> {
		private static String logHDFSFile = "/RawData/opticsjournal/log/" + DateTimeHelper.getNowDate() + ".txt";

		static int cnt = 0;
		private static String lngid = "";
		private static String rawid = "";
		private static String sub_db_id = "";
		private static String product = "";
		private static String sub_db = "";
		private static String provider = "";
		private static String down_date = "";
		private static String batch = "";
		private static String doi = "";
		private static String source_type = "";
		private static String provider_url = "";
		private static String title = "";
		private static String title_alt = "";
		private static String title_sub = "";
		private static String title_series = "";
		private static String keyword = "";
		private static String keyword_alt = "";
		private static String keyword_machine = "";
		private static String clc_no_1st = "";
		private static String clc_no = "";
		private static String clc_machine = "";
		private static String subject_word = "";
		private static String subject_edu = "";
		private static String subject = "";
		private static String abstract_ = ""; // 摘要，因关键字冲突，后面加下划线
		private static String abstract_alt = "";
		private static String abstract_type = "";
		private static String abstract_alt_type = "";
		private static String page_info = "";
		private static String begin_page = "";
		private static String end_page = "";
		private static String jump_page = "";
		private static String doc_code = "";
		private static String doc_no = "";
		private static String raw_type = "";
		private static String recv_date = "";
		private static String accept_date = "";
		private static String revision_date = "";
		private static String pub_date = "";
		private static String pub_date_alt = "";
		private static String pub_place = "";
		private static String page_cnt = "";
		private static String pdf_size = "";
		private static String fulltext_txt = "";
		private static String fulltext_addr = "";
		private static String fulltext_type = "";
		private static String column_info = "";
		private static String fund = "";
		private static String fund_id = "";
		private static String fund_alt = "";
		private static String author_id = "";
		private static String author_1st = "";
		private static String author = "";
		private static String author_raw = "";
		private static String author_alt = "";
		private static String corr_author = "";
		private static String corr_author_id = "";
		private static String email = "";
		private static String subject_dsa = "";
		private static String research_field = "";
		private static String contributor = "";
		private static String contributor_id = "";
		private static String contributor_alt = "";
		private static String author_intro = "";
		private static String organ_id = "";
		private static String organ_1st = "";
		private static String organ= "";
		private static String organ_alt = "";
		private static String preferred_organ = "";
		private static String host_organ_id = "";
		private static String organ_area = "";
		private static String journal_raw_id = "";
		private static String journal_name = "";
		private static String journal_name_alt = "";
		private static String pub_year = "";
		private static String vol = "";
		private static String num = "";
		private static String is_suppl = "";
		private static String issn = "";
		private static String eissn = "";
		private static String cnno = "";
		private static String publisher = "";
		private static String cover_path = "";
		private static String is_oa = "";
		private static String country = "";
		private static String language = "";
		private static String ref_cnt = "";
		private static String ref_id = "";
		private static String cited_id = "";
		private static String cited_cnt = "";
		private static String down_cnt = "";
		private static String is_topcited = "";
		private static String is_hotpaper = "";
		private static String if_html_fulltex = "";
		private static String if_pdf_fulltext = "";
		
		
		//此处为另加字段
		private static String id = "";
		private static String jpg_id= "";
		private static String url = "";
		private static String province_code= "";
		private static String price= "";
		private static String applicant= "";
		private static String creator_release= "";
		private static String description_cycle= "";
		private static String s= "";
		private static String s1= "";
		private static String corr_author_s= "";
		
		public static Map<String, String> codecountryMap = new HashMap<String, String>();
		public static Map<String, String> codelanguageMap = new HashMap<String, String>();
		public static Map<String, String> jidpublisherMap = new HashMap<String, String>();
		public static Map<String, String> Map_info = new HashMap<String, String>();
		public static Map<String, String> map2 = new HashMap<String, String>();
		public static Map<String, String> map = new HashMap<String, String>();
		private static HashMap<String, HashMap<String, String>> journalInfo = new HashMap<String, HashMap<String, String>>();

		public void setup(Context context) throws IOException, InterruptedException {
			batch = context.getConfiguration().get("batch");
			context.getCounter("batch", batch).increment(1);

			JournalInfoMap(context);
		}
		
		
		
		private static void JournalInfoMap(Context context) throws IOException {
			   FileSystem fs = FileSystem.get(context.getConfiguration());
			   FSDataInputStream fin = fs.open(new Path("/RawData/opticsjournal/txt_file/journal_info_txt"));
			
			   //本地读取路径
//			   File file = new File("E:\\dgy_code\\src\\opticsjournal\\journal_info_txt");
//		       FileInputStream fin = new FileInputStream(file);  
			
		       BufferedReader in = null;
			   String line;
			   try {
			    in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
			    while ((line = in.readLine()) != null) {
			     line = line.trim();
//			     Gson gson = new Gson(); 
//			     Type type = new TypeToken<Map<String, String>>(){}.getType();
//				  Map<String, String> mapField = gson.fromJson(line, type);
			     Gson gson = new Gson();
			      
			      map = gson.fromJson(line, map.getClass());
				  String jpg_id = (String)map.get("jpg_id");
				  String dict=map.get("dict");
				  jidpublisherMap.put(jpg_id, dict);
			     
			     
			    }
			   } finally {
			    if (in != null) {
			     in.close();
			    }
			   }
			  }
		

		

		// String doi, String jid,
		public void parseHtml(String htmlText) throws IOException {
			Document doc = Jsoup.parse(htmlText.toString());
			
			//测试 本地读取文件
//			JournalInfoMap(null);
			
			//测试  运行时删除
//			url="http://www.opticsjournal.net/Articles/abstract?aid=OJ170707000049GcJfMi";
//			sub_db_id = "00073";
//			product = "OPTICSJOURNAL";
//			sub_db = "QK";
//			provider = "CLP";
//			down_date = "20190507";
//			pub_year="2017";
//			jpg_id="m00001";
			
			
			//获取rawid
		
			
			//doi
			Element doiElement = doc.select("div#box a[href*=doi]").first();
			if (doiElement != null) {
				doi=doiElement.text().trim();
			}
			
			//provider_url
//			rawid = url.substring(51);
			provider_url = url;
			
			//title
			Element titleElement = doc.select("div.title4 p.ti1").first();
			if (titleElement != null) {
				title=titleElement.text().trim();
			}
			
			//title_alt
			Element title_altElement = doc.select("div.title4 p.ti2").first();
			if (title_altElement != null) {
				title_alt=title_altElement.text().trim();
			}
			
			//keyword  关键词
			Elements keywordElements = doc.select("div.keyword2 ul li");
			if (keywordElements != null) {
				keyword="";
				for (Element keywordElement : keywordElements) {
					String KeyStr = keywordElement.text().trim().replace(";", ",");
					keyword += KeyStr + ";";
				}
			}
			
			//abstract   摘要  abstract_alt 交替文摘
			try {
				Elements abstractElements = doc.getElementById("box").child(0).select("p.zw");
				if ((abstractElements != null) && (abstractElements.size()<=1)) {
					abstract_=abstractElements.first().text().trim();
				}
				else {
					abstract_=abstractElements.get(0).text().trim();
					abstract_alt=abstractElements.get(1).text().trim();
				}
			} catch (Exception e) {
				// TODO: handle exception
			}
			
			//pub_date     无具体日期不填
//			pub_date=pub_year+"0000";
			
			//author
			Elements authorElements = doc.select("div.renwu a.AuthorKey");
			if (authorElements != null) {
				author="";
				for (Element authorElement : authorElements) {
					String KeyStr1 = authorElement.text().trim().replace(";", ",");
					try {
						if (abstract_.substring(0, 10).equals(KeyStr1.substring(0,10)) || abstract_alt.substring(0, 10).equals(KeyStr1.substring(0,10))) {
							System.out.println(KeyStr1);
							KeyStr1="";
						}
					} catch (Exception e) {
						// TODO: handle exception
					}
					author += KeyStr1 + ";";
				}
				if (author.equals("N/A;")) {
					author="";
				}
			}
			

			
			s= jidpublisherMap.get(jpg_id);
			if (s !=null) {

			Gson gson = new Gson();
		    map2 = gson.fromJson(s, map2.getClass());
		      
			
				if (map2.get("book_name") != null) {
			    	title_series=map2.get("book_name");
				}else {
					title_series="";
				}
				//journal_name_alt   期刊第二语言  数据库中获取source_en
				if (map2.get("source_en") != null) {
					journal_name_alt=map2.get("source_en");
				}else {
					journal_name_alt="";
				}
				//issn      数据库中获取identifier_pissn
				if (map2.get("identifier_pissn") != null) {
					issn=map2.get("identifier_pissn");
				}else {
					issn="";
				}
				//cnno      数据库中获取identifier_cnno
				if (map2.get("identifier_cnno") != null) {
					cnno=map2.get("identifier_cnno");
				}else {
					cnno="";
				}
				//publisher   数据库中获取publisher
				if (map2.get("publisher") != null) {
					publisher=map2.get("publisher");
				}else {
					publisher="";
				}
				//province_code   邮编  数据库中获取province_code.........zt有
				if (map2.get("province_code") != null) {
					province_code=map2.get("province_code");
				}else {
					province_code="";
				}
				
				//price   数据库中获取price...........zt有
				if (map2.get("price") != null) {
					price=map2.get("price");
				}else {
					price="";
				}
				
				//journal_raw_id   数据库中获取book_id
				if (map2.get("book_id") != null) {
					journal_raw_id=map2.get("book_id");
				}else {
					journal_raw_id="";
				}
				
				//applicant   从数据库中获取
				if (map2.get("applicant") != null) {
					applicant=map2.get("applicant");
				}else {
					applicant="";
				}
				
				//creator_release  主办单位
				if (map2.get("creator_release") != null) {
					creator_release=map2.get("creator_release");
				}else {
					creator_release="";
				}
				
				//description_cycle 出版周期
				if (map2.get("description_cycle") != null) {
					description_cycle=map2.get("description_cycle");
				}else {
					description_cycle="";
				}
				
				//journal_name   同title_series
				journal_name=title_series;
			}

		    
			
			
			//ref_cnt    引用文章数量
			try {
				Elements ref_cntElements = doc.getElementById("box").child(2).select("p.zw");
				if (ref_cntElements != null) {
					ref_cnt=ref_cntElements.size()+"";
				}
			} catch (Exception e) {
				// TODO: handle exception
			}
			
			//cited_cnt   被引次数
			try {
				Elements cited_cntElements = doc.getElementById("box").child(3).select("p a");
				if (cited_cntElements != null) {
					cited_cnt=cited_cntElements.size()+"@"+down_date;
				}
			} catch (Exception e) {
				// TODO: handle exception
			}
			
			//author_1st
			if (author != null) {
				if (author.contains(";")) {
					if (author.split(";").length >1) {
						author_1st=author.split(";")[0].toString();
					}
					
				} else {
					author_1st=author;
				}
				
			}
			
			
			//clc_no  中图分类号
			try {
				Element clc_noElements = doc.getElementById("box").child(1).select("p.zw:contains(中图分类号)").first();
				if (clc_noElements != null) {
					clc_no=clc_noElements.text().trim().substring(6);
					if (clc_no.split(";").length > 1) {
						clc_no_1st=clc_no.split(";")[0];
					} else {
						clc_no_1st=clc_no;
					}
				}
			} catch (Exception e) {
				// TODO: handle exception
			}
			
			//column_info   所属栏目
			try {
				Element column_infoElements = doc.getElementById("box").child(1).select("p.zw:contains(所属栏目) a").first();
				if (column_infoElements != null) {
					column_info=column_infoElements.text().trim();
				}
			} catch (Exception e) {
				// TODO: handle exception
			}
			if (column_info.equals("目录") || column_info.equals("总目录")) {
				column_info="";
			}
			
			//fund  基金项目
			try {
				Element fundElements = doc.getElementById("box").child(1).select("p.zw:contains(基金项目)").first();
				if (fundElements != null) {
					fund=fundElements.text().trim().substring(5);
				}
			} catch (Exception e) {
				// TODO: handle exception
			}
			
			//recv_date   收稿日期
			try {
				Element recv_dateElements = doc.getElementById("box").child(1).select("p.zw:contains(收稿日期)").first();
				if (recv_dateElements != null) {
					recv_date=recv_dateElements.text().trim().substring(5).replaceAll("-", "");
				}
			} catch (Exception e) {
				// TODO: handle exception
			}
			
			//revision_date 收改稿日期
			try {
				Element revision_dateElements = doc.getElementById("box").child(1).select("p.zw:contains(修改稿日期)").first();
				if (revision_dateElements != null) {
					revision_date=revision_dateElements.text().trim().substring(6).replaceAll("-", "");
				}
			} catch (Exception e) {
				// TODO: handle exception
			}
			
			//pub_date_alt   网络出版日期
//			try {
//				Element pub_date_altElements = doc.getElementById("box").child(1).select("p.zw:contains(网络出版日期)").first();
//				if (pub_date_altElements != null) {
//					pub_date_alt=pub_date_altElements.text().trim().substring(7).replaceAll("-", "");
//				}
//			} catch (Exception e) {
//				// TODO: handle exception
//			}
			
			//accept_date   接受日期
			try {
				Element accept_dateElements = doc.getElementById("box").child(1).select("p.zw:contains(录用日期)").first();
				if (accept_dateElements != null) {
					accept_date=accept_dateElements.text().trim().substring(5).replaceAll("-", "");
				}
			} catch (Exception e) {
				// TODO: handle exception
			}
			
			//author_intro   作者简介
			try {
				Element author_introElements = doc.getElementById("box").child(1).select("p.zw:contains(备注)").first();
				if (author_introElements != null) {
					author_intro=author_introElements.text().trim().substring(3);
				}
			} catch (Exception e) {
				// TODO: handle exception
			}
			
			//corr_author   通讯作者     email  jai@simm.ac.cn:Jing Ai;mygeng@simm.ac.cn:Meiyu Geng;
			try {
				Element corr_authorElements = doc.getElementById("box").child(1).select("p.zw a.acontactorau").first();
				corr_author_s=corr_authorElements.text().trim();
				if (corr_authorElements != null) {
					if (corr_authorElements.text().trim().contains(";")) {
						String[] corr_author_list=corr_authorElements.text().trim().split(";");
						for (String corr : corr_author_list) {
							if (corr!=null) {
								corr=corr.replace("\\)", "");
								if (corr.contains("\\(")) {
									String zuozhe=corr.split("\\(")[0];
									String zuozhe_email=corr.split("\\(")[1];
									corr_author +=zuozhe+";";
									email+=zuozhe_email+":"+zuozhe+";";
								}else if (corr.contains("\\|")) {
									String zuozhe=corr.split("\\|")[0];
									String zuozhe_email=corr.split("\\|")[1];
									corr_author +=zuozhe+";";
									email+=zuozhe_email+":"+zuozhe+";";
								}
							}
						}
					} else {
						String corr=corr_authorElements.text().trim();
						corr_author=corr.replaceAll("\\)","").split("\\(")[0];
						email=corr.replaceAll("\\)","").split("\\(")[1]+":"+corr_author+";";
						s1=email;
					}
				}
				
			} catch (Exception e) {
				// TODO: handle exception
			}

			//organ   organ_1st  author  author_1st
			try {				
				Elements organElements = doc.getElementById("box").child(1).select("div.audpt");
				
				if (organElements != null) {
					LinkedHashMap organmap = new LinkedHashMap();
						String string1[]=organElements.html().split("<br>");
						for (String string2 : string1) {
							String string3[]=string2.trim().split("：");
							if (string3[0] != "" ||string3[1] != "") {
								organmap.put(string3[0].substring(3).replace("</b>", ""),string3[1]);
							}
						}
//						System.out.println(organmap);
						String l1[]=AuthorOrgan.numberByMap(organmap);
						organ=l1[1];
						organ_1st=l1[1].split(";")[0].substring(3);
						author=l1[0];
						author_1st=l1[0].split(";")[0].replace("[1]", "");
				}
			} catch (Exception e) {
				// TODO: handle exception
			}
			
			//if_html_fulltex    是否有html全文
			try {
				Element if_html_fulltexElements = doc.getElementById("box").child(0).select("div.download a:contains(HTML)").first();
				if (if_html_fulltexElements != null) {
					if (if_html_fulltexElements.text().trim().equals("HTML全文")) {
						if_html_fulltex="1";
					} else {
						if_html_fulltex="0";
					}
				}
			} catch (Exception e) {
				// TODO: handle exception
			}
			
			//is_oa    是否OA
			try {
				Element is_oaElements = doc.getElementById("box").child(0).select("div.download a:contains(FREE(OA))").first();
				is_oa="0";
				if (is_oaElements != null) {
					if (is_oaElements.text().trim().equals("FREE(OA)")) {
						is_oa="1";
					}
				}
			} catch (Exception e) {
				// TODO: handle exception
			}
			
			
			
			
			
			}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			{

				lngid = "";
				rawid = "";
				sub_db_id = "00073";
				product = "OPTICSJOURNAL";
				sub_db = "QK";
				provider = "CLP";
				down_date = "20190507";
//					batch = "";			// 已在 setup 内初始化，无需再改
				doi = "";
				source_type = "3";
				provider_url = "";
				title = "";
				title_alt = "";
				title_sub = "";
				title_series = "";
				keyword = "";
				keyword_alt = "";
				keyword_machine = "";
				clc_no_1st = "";
				clc_no = "";
				clc_machine = "";
				subject_word = "";
				subject_edu = "";
				subject = "";
				abstract_ = "";
				abstract_alt = "";
				abstract_type = "";
				abstract_alt_type = "";
				page_info = "";
				begin_page = "";
				end_page = "";
				jump_page = "";
				doc_code = "";
				doc_no = "";
				raw_type = "";
				recv_date = "";
				accept_date = "";
				revision_date = "";
				pub_date = "";
				pub_date_alt = "";
				pub_place = "";
				page_cnt = "";
				pdf_size = "";
				fulltext_txt = "";
				fulltext_addr = "";
				fulltext_type = "";
				column_info = "";
				fund = "";
				fund_id = "";
				fund_alt = "";
				author_id = "";
				author_1st = "";
				author = "";
				author_raw = "";
				author_alt = "";
				corr_author = "";
				corr_author_id = "";
				email = "";
				subject_dsa = "";
				research_field = "";
				contributor = "";
				contributor_id = "";
				contributor_alt = "";
				author_intro = "";
				organ_id = "";
				organ_1st = "";
				organ = "";
				organ_alt = "";
				preferred_organ = "";
				host_organ_id = "";
				organ_area = "";
				journal_raw_id = "";
				journal_name = "";
				journal_name_alt = "";
				pub_year = "";
				vol = "";
				num = "";
				is_suppl = "";
				issn = "";
				eissn = "";
				cnno = "";
				publisher = "";
				cover_path = "";
				is_oa = "";
				country = "CN";
				language = "ZH";
				ref_cnt = "";
				ref_id = "";
				cited_id = "";
				cited_cnt = "";
				down_cnt = "";
				is_topcited = "";
				is_hotpaper = "";
				if_html_fulltex = "";
				if_pdf_fulltext = "1";
				corr_author_s="";
				s1="";

			}

			cnt += 1;
			if (cnt == 1) {
				System.out.println("text:" + value.toString());
			}

			String text = value.toString().trim();

			Gson gson = new Gson();
			Type type = new TypeToken<Map<String, Object>>() {
			}.getType();

			Map<String, Object> mapField = gson.fromJson(text, type);
			rawid = mapField.get("raw_id").toString();
			jpg_id=mapField.get("jpg_id").toString();
			pub_year=mapField.get("year").toString();
			vol=mapField.get("juan").toString();
			num=mapField.get("qi").toString();
			url=mapField.get("url").toString();
			
		
			
			String htmlText = mapField.get("html").toString();
			
//			if (map2.size() < 1) {
//				context.getCounter("map", "Error: map2").increment(1);
//				LogMR.log2HDFS4Mapper(context, logHDFSFile, "error: map2 " + map2+jpg_id+url);
//				return;
//			}
			
			if (map.size() < 1) {
				context.getCounter("map", "Error: map").increment(1);
				LogMR.log2HDFS4Mapper(context, logHDFSFile, "map ");
				return;
			}

			parseHtml(htmlText);
			if(title.contains("一种中继网络中分布式空时分组码的设计")) {
				context.getCounter("map", "title count").increment(1);
			}

			if (title.length() < 1) {
				context.getCounter("map", "Error: no title").increment(1);
				LogMR.log2HDFS4Mapper(context, logHDFSFile, "Error: no title: " + title);
				return;
			}
			if (rawid.length() < 1) {

				context.getCounter("map", "Error: no rawid").increment(1);
				LogMR.log2HDFS4Mapper(context, logHDFSFile, "error: no rawid " + rawid);
				return;
			}
			lngid = VipIdEncode.getLngid(sub_db_id, rawid, false);

			
			// 处理作者机构末尾;号
			author = author.replaceAll(";$", "");
			organ = organ.replaceAll(";$", "");
			subject = subject.replaceAll(";$", "");
			keyword=keyword.replaceAll(";$", "");
			fund=fund.replaceAll("\\.$","").replaceAll("。", "").replaceAll("、", ";").replaceAll("和", ";");
			corr_author=corr_author.replaceAll(";$", "");
			email=email.replaceAll(";$", "");
			LogMR.log2HDFS4Mapper(context, logHDFSFile, "s1:"+s1);
			

			
			organ_1st = organ_1st.replaceAll("^\\[.*?\\]", "");
			author_1st = author_1st.replaceAll("\\[.*?\\]$", "");
			
			//处理中图分类号中的空格
			clc_no=clc_no.replaceAll(" ", "");
			clc_no_1st=clc_no_1st.replaceAll(" ", "");

			XXXXObject xObj = new XXXXObject();
			{
				xObj.data.put("lngid", lngid);
				xObj.data.put("rawid", rawid);
				xObj.data.put("sub_db_id", sub_db_id);
				xObj.data.put("product", product);
				xObj.data.put("sub_db", sub_db);
				xObj.data.put("provider", provider);
				xObj.data.put("down_date", down_date);
				xObj.data.put("batch", batch);
				xObj.data.put("doi", doi);
				xObj.data.put("source_type", source_type);
				xObj.data.put("provider_url", provider_url);
				xObj.data.put("title", title);
				xObj.data.put("title_alt", title_alt);
				xObj.data.put("title_sub", title_sub);
				xObj.data.put("title_series", title_series);
				xObj.data.put("keyword", keyword);
				xObj.data.put("keyword_alt", keyword_alt);
				xObj.data.put("keyword_machine", keyword_machine);
				xObj.data.put("clc_no_1st", clc_no_1st);
				xObj.data.put("clc_no", clc_no);
				xObj.data.put("clc_machine", clc_machine);
				xObj.data.put("subject_word", subject_word);
				xObj.data.put("subject_edu", subject_edu);
				xObj.data.put("subject", subject);
				xObj.data.put("abstract", abstract_);
				xObj.data.put("abstract_alt", abstract_alt);
				xObj.data.put("abstract_type", abstract_type);
				xObj.data.put("abstract_alt_type", abstract_alt_type);
				xObj.data.put("page_info", page_info);
				xObj.data.put("begin_page", begin_page);
				xObj.data.put("end_page", end_page);
				xObj.data.put("jump_page", jump_page);
				xObj.data.put("doc_code", doc_code);
				xObj.data.put("doc_no", doc_no);
				xObj.data.put("raw_type", raw_type);
				xObj.data.put("recv_date", recv_date);
				xObj.data.put("accept_date", accept_date);
				xObj.data.put("revision_date", revision_date);
				xObj.data.put("pub_date", pub_date);
				xObj.data.put("pub_date_alt", pub_date_alt);
				xObj.data.put("pub_place", pub_place);
				xObj.data.put("page_cnt", page_cnt);
				xObj.data.put("pdf_size", pdf_size);
				xObj.data.put("fulltext_txt", fulltext_txt);
				xObj.data.put("fulltext_addr", fulltext_addr);
				xObj.data.put("fulltext_type", fulltext_type);
				xObj.data.put("column_info", column_info);
				xObj.data.put("fund", fund);
				xObj.data.put("fund_id", fund_id);
				xObj.data.put("fund_alt", fund_alt);
				xObj.data.put("author_id", author_id);
				xObj.data.put("author_1st", author_1st);
				xObj.data.put("author", author);
				xObj.data.put("author_raw", author_raw);
				xObj.data.put("author_alt", author_alt);
				xObj.data.put("corr_author", corr_author);
				xObj.data.put("corr_author_id", corr_author_id);
				xObj.data.put("email", email);
				xObj.data.put("subject_dsa", subject_dsa);
				xObj.data.put("research_field", research_field);
				xObj.data.put("contributor", contributor);
				xObj.data.put("contributor_id", contributor_id);
				xObj.data.put("contributor_alt", contributor_alt);
				xObj.data.put("author_intro", author_intro);
				xObj.data.put("organ_id", organ_id);
				xObj.data.put("organ_1st", organ_1st);
				xObj.data.put("organ", organ);
				xObj.data.put("organ_alt", organ_alt);
				xObj.data.put("preferred_organ", preferred_organ);
				xObj.data.put("host_organ_id", host_organ_id);
				xObj.data.put("organ_area", organ_area);
				xObj.data.put("journal_raw_id", journal_raw_id);
				xObj.data.put("journal_name", journal_name);
				xObj.data.put("journal_name_alt", journal_name_alt);
				xObj.data.put("pub_year", pub_year);
				xObj.data.put("vol", vol);
				xObj.data.put("num", num);
				xObj.data.put("is_suppl", is_suppl);
				xObj.data.put("issn", issn);
				xObj.data.put("eissn", eissn);
				xObj.data.put("cnno", cnno);
				xObj.data.put("publisher", publisher);
				xObj.data.put("cover_path", cover_path);
				xObj.data.put("is_oa", is_oa);
				xObj.data.put("country", country);
				xObj.data.put("language", language);
				xObj.data.put("ref_cnt", ref_cnt);
				xObj.data.put("ref_id", ref_id);
				xObj.data.put("cited_id", cited_id);
				xObj.data.put("cited_cnt", cited_cnt);
				xObj.data.put("down_cnt", down_cnt);
				xObj.data.put("is_topcited", is_topcited);
				xObj.data.put("is_hotpaper", is_hotpaper);
				xObj.data.put("if_html_fulltex", if_html_fulltex);
				xObj.data.put("if_pdf_fulltext", if_pdf_fulltext);
				
				//zt新增
				xObj.data.put("province_code", province_code);
				xObj.data.put("price", price);
				xObj.data.put("applicant", applicant);
				xObj.data.put("creator_release", creator_release);
				xObj.data.put("description_cycle", description_cycle);
			}

			byte[] bytes = com.process.frame.util.VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(rawid), new BytesWritable(bytes));
			context.getCounter("map", "count").increment(1);

		}
	}
}
