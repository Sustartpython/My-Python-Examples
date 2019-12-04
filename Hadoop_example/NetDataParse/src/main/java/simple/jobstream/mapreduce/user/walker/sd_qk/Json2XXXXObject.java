package simple.jobstream.mapreduce.user.walker.sd_qk;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
//import org.apache.tools.ant.taskdefs.Replace;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.vip.LogMR;
import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;

// 将html格式转化为XXXXObject格式，包含去重合并
public class Json2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static String inputHdfsPath = "";
	private static String outputHdfsPath = ""; // 这个目录会被删除重建
	private static int reduceNum = 0;

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
		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.9f);
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

	public static class ProcessMapper extends SciencedirectMapper {

		int cnt = 0;
		int sourceNullCnt = 0;
		int issnNullCnt = 0;
		
		@Override
		void setJournalInfoFile() {
			journalInfoFile =  "/RawData/elsevier/sd_qk/ref_file/journal_info.json";			
		}
		
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.getCounter("map", "input").increment(1);
			// 防止混乱与错位
			{
				lngid = "";
				rawid = "";
				sub_db_id = "";
				product = "";
				sub_db = "";
				provider = "";
				down_date = "";
//	    		batch = ""; 
				translator = "";
				translator_intro = "";
				ori_src = "";
				doi = "";
				source_type = "";
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
				coden = "";
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
				subject_major = "";
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
				isbn = "";
				publisher = "";
				cover_path = "";
				is_oa = "";
				country = "";
				language = "";
				ref_cnt = "";
				ref_id = "";
				cited_id = "";
				cited_cnt = "";
				down_cnt = "";
				orc_id = "";
				researcher_id = "";
				is_topcited = "";
				is_hotpaper = "";
				pubmed_id = "";
				pmc_id = "";

			}

			String line = value.toString().trim();
			Gson gson = new Gson();
			Type type = new TypeToken<Map<String, String>>() {
			}.getType();
			Map<String, String> mapJson = gson.fromJson(line, type);
			rawid = mapJson.get("rawid");
			down_date = mapJson.get("down_date");
			journal_raw_id = mapJson.get("jid");
			journal_name = mapJson.get("jname");
			pub_year = mapJson.get("pubyear");
			language = "EN";
			country = "NL"; // NL（荷兰）
			provider_url = "https://www.sciencedirect.com/science/article/pii/" + rawid;
			product = "SCIENCEDIRECT";
			provider = "ELSEVIER";		
			publisher = "Elsevier Science";		// 出版社给个默认值
			source_type = "3";
			
			String html = mapJson.get("detail");

			Document doc = Jsoup.parse(html);
			if (doc.select("div.article-wrapper > article > div.Publication").first() != null) {
				// 基本上都是 ArticleTitle 了
				// S0892059189706212: #react-root > div > div > div > div > section > div >
				// div.article-wrapper > div.cl-m-3-3.cl-t-6-9.cl-l-6-12.pad-left.pad-right >
				// div.Publication > div.publication-vol > h2 > span > a
				// S0140673600025459: #react-root > div > div > div > div > section > div >
				// div.article-wrapper > div.cl-m-3-3.cl-t-6-9.cl-l-6-12.pad-left.pad-right >
				// div.Publication > div.publication-brand > a
				parseArticleTitle(rawid, doc);
				context.getCounter("map", "ArticleTitle").increment(1);
			} else {
				context.getCounter("map", "error feature").increment(1);
				LogMR.log2HDFS4Mapper(context, logHDFSFile, "error feature:" + rawid);
				return;
			}
			
			// 从期刊信息表获取数据
			if (journalInfo.containsKey(journal_raw_id)) {
				journal_name = journalInfo.get(journal_raw_id).get("jname");
				
				String contentType = journalInfo.get(journal_raw_id).get("content_type").trim();
				context.getCounter("map", "content_type: " +  contentType).increment(1);
				if (contentType.equalsIgnoreCase("JL")) {
					sub_db = "QK";
					sub_db_id = "00018";
				}
				else if (contentType.equalsIgnoreCase("BS")) {
					sub_db = "BS";
					sub_db_id = "00109";
				}
				else {
					context.getCounter("map", "err content_type: " +  contentType).increment(1);
					return;
				}
			}
			else {
//				context.getCounter("map", "err journal_raw_id: " + journal_raw_id).increment(1);
				context.getCounter("map", "err journal_raw_id").increment(1);
				LogMR.log2HDFS4Mapper(context, logHDFSFile, "err journal_raw_id: " + journal_raw_id);
				return;
			}
			lngid = VipIdEncode.getLngid(sub_db_id, rawid, false);

			// 没有title的数据不能要
			if (title.length() < 1) {
				context.getCounter("map", "no title").increment(1);
				LogMR.log2HDFS4Mapper(context, logHDFSFile, "no title: " + rawid);
				return;
			}

			// issn不应该不存在
			if (issn.length() < 1) {
				LogMR.log2HDFS4Mapper(context, logHDFSFile, "issnNull text:" + value.toString());
				context.getCounter("map", "issn.length() < 1").increment(1);
				LogMR.log2HDFS4Mapper(context, logHDFSFile, "no issn: " + rawid);
				return;
			}
			
//			int int_pub_year = 0;
//			try {
//				int_pub_year = Integer.parseInt(pub_year);
//			} catch (Exception e) {
//				context.getCounter("map", "err_int_pub_year").increment(1);
//				LogMR.log2HDFS4Mapper(context, logHDFSFile, "err_int_pub_year:" + pub_year + ";" + rawid);
//				return;
//			}

			if ((Integer.parseInt(pub_year) < 1000) || (Integer.parseInt(pub_year) > curYear + 1)) {
				context.getCounter("map", "err_date").increment(1);
				LogMR.log2HDFS4Mapper(context, logHDFSFile, "err_date:" + pub_year + ";" + rawid);
				return;
			}

			// S0140673601985642
			if (pub_year.equals("1900")) {
				context.getCounter("map", "warning:date_1900").increment(1);
				LogMR.log2HDFS4Mapper(context, logHDFSFile, "date_1900:" + rawid);
			}

			// 一些统计数据
			{
				if (journal_name.length() < 1) {
					sourceNullCnt += 1;
					if (sourceNullCnt < 3) {
						LogMR.log2HDFS4Mapper(context, logHDFSFile, "sourceNull text:" + value.toString());
					}
					context.getCounter("map", "journal_name.length() < 1").increment(1);
				}

			}

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
				xObj.data.put("translator", translator);
				xObj.data.put("translator_intro", translator_intro);
				xObj.data.put("ori_src", ori_src);
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
				xObj.data.put("coden", coden);
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
				xObj.data.put("subject_major", subject_major);
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
				xObj.data.put("isbn", isbn);
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
				xObj.data.put("orc_id", orc_id);
				xObj.data.put("researcher_id", researcher_id);
				xObj.data.put("is_topcited", is_topcited);
				xObj.data.put("is_hotpaper", is_hotpaper);
				xObj.data.put("pubmed_id", pubmed_id);
				xObj.data.put("pmc_id", pmc_id);
			}
			context.getCounter("map", "out").increment(1);

			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(rawid), new BytesWritable(bytes));
		}		
	}
}
