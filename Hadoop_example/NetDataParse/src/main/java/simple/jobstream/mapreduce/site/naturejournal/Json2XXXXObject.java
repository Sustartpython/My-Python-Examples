package simple.jobstream.mapreduce.site.naturejournal;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.util.StringHelper;
import simple.jobstream.mapreduce.common.vip.LogMR;
import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;

/**
 * <p>
 * Description: 针对kns和海外版合版的解析
 * http://oversea.cnki.net/kcms/detail/detail.aspx?filename=KYGL2018S1010&dbcode=CJFD
 * </p>
 * 
 * @author qiuhongyang 2018年11月6日 下午1:36:17
 */
public class Json2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 0;

	private static String inputHdfsPath = "";
	private static String outputHdfsPath = "";

	public void pre(Job job) {
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
		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.7f);
		System.out.println("******mapred.reduce.slowstart.completed.maps*******"
				+ job.getConfiguration().get("mapred.reduce.slowstart.completed.maps"));
		job.getConfiguration().set("io.compression.codecs",
				"org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(UniqXXXXObjectReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);

		// job.setInputFormatClass(SimpleTextInputFormat.class);
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
		private static String organ = "";
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

		private static HashMap<String, HashMap<String, String>> journalInfo = new HashMap<String, HashMap<String, String>>();

	
		public void setup(Context context) throws IOException, InterruptedException {
			   batch = context.getConfiguration().get("batch");
			   context.getCounter("batch", batch).increment(1);
			  }
		

		public void parsehtml(String detailHtml) {
			Document doc = Jsoup.parse(detailHtml);
			
			Element spanElement = doc.select("span[class = inline-block pl20 pt5 pb5 icon icon-right]").first();
			if (spanElement != null) {
				fulltext_type = "pdf";
			}
			//title
			Element h1 = doc.select("h1[class = tighten-line-height small-space-below]").first();
			if (h1 != null) {
				title = h1.text();
			}
			//vol
			Element citation_volume = doc.select("meta[name = prism.volume]").first();
			if (citation_volume!= null) {
				vol = citation_volume.attr("content");
			}
			//num
			Element citation_issue = doc.select("meta[name = prism.number]").first();
			if (citation_issue!= null) {
				num = citation_issue.attr("content");
			}
			//期刊刊名ID
			Element journal_id = doc.select("meta[name = journal_id]").first();
			if (journal_id!= null) {
				journal_raw_id = journal_id.attr("content");
			}
			//doi
			Element prism_doi = doc.select("meta[name = prism.doi]").first();
			if (prism_doi!= null) {
				doi = prism_doi.attr("content").replace("doi:", "");
			}
			//issn
			Element citation_issn = doc.select("meta[name = prism.issn]").first();
			if (citation_issn!= null) {
				issn = citation_issn.attr("content");
			}
			//出版时间
			Element citation_online_date = doc.select("meta[name = prism.publicationDate]").first();
			if (citation_online_date!= null) {
				pub_date = citation_online_date.attr("content");
				pub_date = pub_date.replace("-", "");
				if (pub_date.equals("")) {
					pub_date = "19000000";
				}
				pub_year = pub_date.substring(0,4);
			}
			//publisher
			Element citation_publisher = doc.select("meta[name = dc.publisher]").first();
			if (citation_publisher!= null) {
				publisher = citation_publisher.attr("content");
			}
			
			//jurnal_name
			Element citation_journal_title = doc.select("meta[name = prism.publicationName]").first();
			if (citation_journal_title!= null) {
				journal_name = citation_journal_title.attr("content");
			}
			//作者  机构
			Elements all_li = doc.select("li[itemprop = author]");
			if (all_li != null) {
				int orgaNum = 1; 
				int i = 1;
				ArrayList list = new ArrayList();
				String organtemp = "";
				for (Element li : all_li) {
					int[] score = new int[500];
					Element span_name = li.select("a").first();
					if (span_name != null) {
						Elements spans = li.select("span[itemprop=affiliation]");
						if (spans != null) {
							for (Element element : spans) {
								Element meta = element.select("meta[itemprop=name]").first();
								if (meta != null) {
									organtemp = meta.attr("content").trim().replace("'", "''");
									if (list.indexOf(organtemp)>-1) {
										score[i] = list.indexOf(organtemp)+1; 
//										orgaNum = list.indexOf(organtemp);
//										i+=1;
										continue;
									}
									if (organtemp.length() == 0) {
										continue;
									}
									organ = organ+"["+orgaNum+"]"+organtemp+";";
									score[i]=orgaNum;
									list.add(organtemp);
									i+=1;
									orgaNum+=1; 
								}
								else {
									continue;
								}
								
							}
							String line =""; 
							for (int j = 0; j < score.length; j++) {
								if (score[j]>0){
									line +=score[j]+",";
								}
								
								} 
							if (line.equals("")) {
								author = author+span_name.text()+";";
							}
							else {
								line = line.substring(0,line.length()-1);
								author = author+span_name.text()+"["+(line)+"]"+";";
							}
						}
						
					}
					
				}
				
			}
			if (author.length() != 0 && author.endsWith(";")) {
				author = author.substring(0, author.length() - 1);
				String line = author.split(";")[0];
				if (line.endsWith("]")) {
					author_1st = line.split("\\[")[0];
				}
				else {
					author_1st = line;
				}
			    }
			
			if (organ.length() != 0 && organ.endsWith(";")) {
				organ = organ.substring(0, organ.length() - 1);
				organ_1st = organ.split(";")[0];
				organ_1st = organ_1st.replace("[1]", "");
			    }
			//摘要
			Element div = doc.select("div[class = pl20 mq875-pl0 js-collapsible-section]").first();
			if (div!= null) {
				abstract_ = div.text().trim();
			}
			//关键词
			Element cleared = doc.select("div[data-component = article-subject-links]").first();
			if (cleared!= null) {
				
				Elements lis = cleared.select("li");
				if (lis != null) {
					for (Element li : lis) {
						keyword = keyword+li.text().trim()+";";
					}
				}
			}
			
			if (keyword.length() != 0 && keyword.endsWith(";")) {
				keyword = keyword.substring(0, keyword.length() - 1);
			    }
			
			//通讯作者
			Element a = doc.select("a[id = corresp-c1]").first();
			if (a != null) {
				corr_author = a.text().trim();
			}
			//引文数
			Element divy = doc.select("div[id = references-content]").first();
			if (divy != null) {
				Elements liElements = divy.select("li[class = small-space-below border-gray-medium border-bottom-1 position-relative js-ref-item]");
				
				ref_cnt = Integer.toString(liElements.toArray().length);
			}
			Element timeElement = doc.select("div[class = grid grid-12]").first();
			if (timeElement != null) {
				Elements timedivs = timeElement.select("div");
				if (timedivs != null) {
					for (Element timediv : timedivs) {
						//收稿（提交）日期。
						if (timediv.text().contains("Received")) {
							recv_date = timediv.select("time").attr("datetime");
							recv_date = recv_date.replace("-", "");
						}
						//接受日期
						if (timediv.text().contains("Accepted")) {
							accept_date	 = timediv.select("time").attr("datetime");
							accept_date	 = accept_date	.replace("-", "");
						}
					}
				}
			}
			//引文条数
			Elements pElement = doc.select("p[class = pa10 pt0 pb0 mb0 pin-left]");
			if (pElement != null) {
				for (Element p : pElement) {
					if (p.text().equals("Citations")) {
						Element strong = p.select("strong").first();
					}
				}
			}
			
			Element spanfirst = doc.select("span[itemprop=pageStart]").first();
			if (spanfirst != null) {
				begin_page = spanfirst.text();
			}
			Element spanend = doc.select("span[itemprop=pageEnd]").first();
			if (spanend != null) {
				end_page = spanend.text();
			}
			if (begin_page.equals("")) {
				page_info = end_page;
			}
			if (end_page.equals("")) {
				end_page = begin_page;
			}
			
			page_info = begin_page+"-"+end_page;
			if (page_info.equals("-")) {
				page_info = "";
			}
		}



		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			cnt += 1;
			if (cnt == 1) {
				System.out.println("text:" + value.toString());
			}

			{
				lngid = "";
				rawid = "";
				sub_db_id = "00035";
				product = "NATURE";
				sub_db = "QK";
				provider = "NATURE";
				down_date = "20190101";
//				batch = "";			// 已在 setup 内初始化，无需再改
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
				country = "UK";
				language = "EN";
				ref_cnt = "";
				ref_id = "";
				cited_id = "";
				cited_cnt = "";
				down_cnt = "";
				is_topcited = "";
				is_hotpaper = "";
			}

			String line = value.toString().trim();

			Gson gson = new Gson();
			Type type = new TypeToken<Map<String, String>>() {
			}.getType();
			Map<String, String> mapJson = gson.fromJson(line, type);
			rawid = mapJson.get("id");
			provider_url = "https://www.nature.com/articles/" + rawid;
			String knsHtml = mapJson.get("html");

			

			parsehtml(knsHtml); // 解析 kns 详情页


			if ((title.length() < 1) && (title_alt.length() < 1)) {
				context.getCounter("map", "error: no title").increment(1);
				return;
			}

			lngid = VipIdEncode.getLngid("00035", rawid, false);
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
			}

			context.getCounter("map", "count").increment(1);

			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(rawid), new BytesWritable(bytes));
		}
	}

}
