package simple.jobstream.mapreduce.site.bioonejournal;

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
import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;
import simple.jobstream.mapreduce.common.vip.AuthorOrgan;
import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;
import simple.jobstream.mapreduce.site.cssci.Json2XXXXObject.ProcessMapper;
import simple.jobstream.mapreduce.common.util.DateTimeHelper;

//统计wos和ei的数据量
public class Json2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 50;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";

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
		job.getConfiguration().set("io.compression.codecs",
				"org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
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

	// ======================================处理逻辑=======================================
	//继承Mapper接口,设置map的输入类型为<Object,Text>
	//输出类型为<Text,IntWritable>
	public static class ProcessMapper extends Mapper<LongWritable, Text, Text, BytesWritable> {
		public String batch = "";				
		
		public void setup(Context context) throws IOException,
			InterruptedException {
			batch = context.getConfiguration().get("batch");
		}
		
		
		
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	
	    	Gson gson = new Gson();
	    	Type type = new TypeToken<Map<String, String>>(){}.getType();
	    	Map<String, String> mapJson = gson.fromJson(value.toString(), type);
	    	
	    	String url = mapJson.get("url").trim();
	    	String volume = mapJson.get("vol").trim();
	    	String issue = mapJson.get("issue").trim();
	    	String journal_raw_id = mapJson.get("gch").trim();
	    	String down_date = mapJson.get("down_date").trim();
	    	String html  = mapJson.get("detail").trim();
	    	
			Document doc = Jsoup.parse(html);						
            
			LinkedHashMap<String, String> mapcre_ins = new LinkedHashMap<String, String>();
			String author = "";
			String author_1st = "";
			String organ = "";
			String organ_1st = "";
			String title = "";
			String keyword = "";
			String pub_year = "";
			String vol = "";
			String num = "";
			String journal_name = "";
			String raw_type = "";
			String doi = "";
			String page_info = "";
			String begin_page = "";
			String end_page = "";
			String eissn  = "";
			String pissn = "";
			String recv_date = "";
			String accept_date = "";
			String pub_date = "";
			String publisher = "";
			String abstract_ = "";
			
			String lngid = "";
			String rawid = "";
			String product = "BIOONE";
			String sub_db = "QK";
			String provider = "BIOONE";
			String sub_db_id = "00032";
			String source_type = "3";
			String provider_url = "https://bioone.org" + url;
			String country = "US";
			String language = "EN";
            
            Element titleTag = doc.select("meta[name*=citation_title]").first();
            if (titleTag !=null) {        
            	title = titleTag.attr("content");
            	if (title.equals("")) {
            		titleTag = doc.select("text.ProceedingsArticleOpenAccessHeaderText").first();
            		if (titleTag !=null) {
            			title = titleTag.text().trim();
            		}
				}
			}
            Element dateTag = doc.select("meta[name*=citation_publication_date]").first();
            if (dateTag !=null) {        
            	pub_date = dateTag.attr("content").replace("/", "");
            	if (pub_date.length() == 6) {
            		pub_date = pub_date + "00";
				}
			}
            Element typeTag = doc.select("meta[name*=citation_article_type]").first();
            if (typeTag !=null) {        
            	raw_type = typeTag.attr("content");
			}
            
            Element bgPageTag = doc.select("meta[name*=citation_firstpage]").first();
            if (bgPageTag !=null) {        
            	begin_page = bgPageTag.attr("content");
			}
            Element edPageTag = doc.select("meta[name*=citation_lastpage]").first();
            if (edPageTag !=null) {        
            	end_page = edPageTag.attr("content");
			}
            if (!begin_page.equals("")) {
				if (end_page.equals("")) {
					end_page = begin_page;
				}
				page_info = begin_page + "-" + end_page;
			}
            Element subjectTag = doc.select("meta[name*=citation_keywords]").first();
            if (subjectTag !=null) {        
            	keyword = subjectTag.attr("content").replace("; ", ";").replaceAll(";$", "");
			}
            Element doiTag = doc.select("meta[name*=citation_doi]").first();
            if (doiTag !=null) {        
            	doi = doiTag.attr("content");
			}
            Element sourceTag = doc.select("meta[name*=citation_journal_title]").first();
            if (sourceTag !=null) {        
            	journal_name = sourceTag.attr("content");
			}
            Element volumeTag = doc.select("meta[name*=citation_volume]").first();
            if (volumeTag !=null) {        
            	vol = volumeTag.attr("content");
			}
            if (vol.equals("")) {
            	vol = volume;
			}
            Element issueTag = doc.select("meta[name*=citation_issue]").first();
            if (issueTag !=null) {        
            	num = issueTag.attr("content");
			}
            if (num.equals("")) {
            	num = issue;
			}
            Element publisherTag = doc.select("meta[name*=citation_publisher]").first();
            if (publisherTag !=null) {        
            	publisher = publisherTag.attr("content");
			}
            Element descriptionTag = doc.select("meta[name=citation_abstract]").first();
            if (descriptionTag !=null) {
            	abstract_ = descriptionTag.attr("content");
			}
        	if (abstract_.equals("")) {
        		descriptionTag = doc.select("#article-body").first();
        		if (descriptionTag !=null) {
        			abstract_ = descriptionTag.text().trim();
				}           		
			}
            for (Element issnTag : doc.select("meta[name*=citation_issn]")) {
				Node next = issnTag.nextSibling();
				if (next.nodeName().equals("#comment")) {
					if (next.toString().trim().equals("<!--ISSN-->")) {
						pissn = issnTag.attr("content");
					}
					else if (next.toString().trim().equals("<!--eISSN-->")) {
						eissn = issnTag.attr("content");
					}
				}
			}
            for (Element authorTag : doc.select("meta[name=citation_author]")){
            	String auth = authorTag.attr("content");
            	if (author_1st.equals("")) {
            		author_1st = auth;
				}
            	String org = "";
            	Element next = authorTag.nextElementSibling();
            	while (next!=null && next.attr("name").equals("citation_author_institution")) {
					org = org + next.attr("content").replace(":;", ":").replace(";",",")+";";
					next = next.nextElementSibling();
				}
            	org = org.replaceAll(";$", "");
            	if (organ_1st.equals("")) {
            		organ_1st = org;
				}
            	mapcre_ins.put(auth, org);
            }
            String[] result = AuthorOrgan.numberByMap(mapcre_ins);
            author = result[0];
            organ = result[1];
            
            Element recvdateTag = doc.select("#articleSubmission > text").first();
            if (recvdateTag != null) {
				for (String date : recvdateTag.text().split(";")) {
					date = date.trim();
					if (date.startsWith("Received:")) {
						recv_date = date.replace("Received:", "").trim();
						recv_date = DateTimeHelper.stdDate(recv_date);
					}
					else if (date.startsWith("Accepted:")) {
						accept_date = date.replace("Accepted:", "").trim();
						accept_date = DateTimeHelper.stdDate(accept_date);
					}
					else if (date.startsWith("Published:")) {
						pub_date = date.replace("Published:", "").trim();
						pub_date = DateTimeHelper.stdDate(pub_date);
					}
				}
			}
            
            if (!pub_date.equals("")) {
            	pub_year = pub_date.substring(0, 4).trim();
			}
            
            rawid = doi;
            if (rawid.equals("")) {
            	context.getCounter("map", "error rawid").increment(1);
            	return;
			}
            lngid = VipIdEncode.getLngid(sub_db_id,rawid,false);
            journal_raw_id = url.split("/")[2].toLowerCase();
            
            
            
                      
			
            

			XXXXObject xObj = new XXXXObject();

			xObj.data.put("author",author);
			xObj.data.put("author_1st",author_1st);
			xObj.data.put("organ",organ);
			xObj.data.put("organ_1st",organ_1st);
			xObj.data.put("title",title);
			xObj.data.put("keyword",keyword);
			xObj.data.put("pub_year",pub_year);
			xObj.data.put("vol",vol);
			xObj.data.put("num",num);
			xObj.data.put("journal_name",journal_name);
			xObj.data.put("raw_type",raw_type);
			xObj.data.put("doi",doi);
			xObj.data.put("page_info",page_info);
			xObj.data.put("begin_page",begin_page);
			xObj.data.put("end_page",end_page);
			xObj.data.put("eissn",eissn);
			xObj.data.put("pissn",pissn);
			xObj.data.put("recv_date",recv_date);
			xObj.data.put("accept_date",accept_date);
			xObj.data.put("pub_date",pub_date);
			xObj.data.put("publisher",publisher);
			xObj.data.put("abstract",abstract_);

			xObj.data.put("lngid",lngid);
			xObj.data.put("rawid",rawid);
			xObj.data.put("product",product);
			xObj.data.put("sub_db",sub_db);
			xObj.data.put("provider",provider);
			xObj.data.put("sub_db_id",sub_db_id);
			xObj.data.put("source_type",source_type);
			xObj.data.put("provider_url",provider_url);
			xObj.data.put("country",country);
			xObj.data.put("language",language);
			xObj.data.put("journal_raw_id",journal_raw_id);
			xObj.data.put("down_date",down_date);
			xObj.data.put("batch",batch);
			
			

			context.getCounter("map", "count").increment(1);

			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(rawid), new BytesWritable(bytes));
            
	    }

	}
}