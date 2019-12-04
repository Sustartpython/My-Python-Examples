package simple.jobstream.mapreduce.site.acsjournal;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
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
import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;

//统计wos和ei的数据量
public class Json2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 5;
	private static int reduceNum = 100;

//	private static String batch = "";
	private static String inputHdfsPath = "";
	private static String outputHdfsPath = "";

	public void pre(Job job) {
		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
		reduceNum = Integer.parseInt(job.getConfiguration().get("reduceNum"));
		job.setJobName(job.getConfiguration().get("jobName"));
//		batch = job.getConfiguration().get("batch");
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

		if (testRun) {
			job.setNumReduceTasks(testReduceNum);
		} else {
			job.setNumReduceTasks(reduceNum);
		}
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
	// 继承Mapper接口,设置map的输入类型为<Object,Text>
	// 输出类型为<Text,IntWritable>
	public static class ProcessMapper extends Mapper<LongWritable, Text, Text, BytesWritable> {
		
		static String rawid = "";
		static String down_date = "";
		static String batch = "";
		static String doi = "";
		static String title = "";
		static String keyword = "";
		static String description = "";
		static String begin_page = "";
		static String end_page = "";
		static String raw_type = "";
		static String recv_date = "";
		static String accept_date = "";
		static String pub_date = "";
		static String pub_date_alt = "";
		static String author = "";
		static String organ = "";
		static String journal_id = "";
		static String journal_name = "";
		static String pub_year = "";
		static String vol = "";
		static String num = "";
		static String publisher = "";
		static String provider_url = "";

		static String page = "";

		public void setup(Context context) throws IOException, InterruptedException {
			batch = context.getConfiguration().get("batch");
		}

		static String cleanSpace(String text) {
			text = text.replaceAll("[\\s\\p{Zs}]+", " ").trim();
			return text;
		}

		public static boolean parseHtml(Document doc) {

			// title 文章标题
//			String title = "";
			Element titleElement = doc.select("meta[name=dc.Title]").first();
			if (titleElement != null) {
				title = titleElement.attr("content").trim();
			}

			// 文章类型
//			String raw_type = "";
			Element articleTypeElement = doc
					.select("div.widget-body.body.body-none.body-compact-all > h3.manuscriptType").first();
			if (articleTypeElement != null) {
				raw_type = articleTypeElement.text().trim();
			}

			// identifier_doi DOI
//			String identifier_doi = "";
			Element doiElement = doc.select("meta[name=dc.Identifier]").first();
			if (doiElement != null) {
				doi = doiElement.attr("content").trim();
			}

			// 作者
			// 无作者,有作者无单位(无上标),有作者有单位(有上标,1或多) ,默认无作者
//			String author = "";
			Elements author_elements = doc.select("div#articleMeta > div#authors > span.hlFld-ContribAuthor");
			// 有作者
			if (author_elements.size() > 0) {
				for (Element aue : author_elements) {// 取所有作者标签
					String author_name = "";
					Element author_name_element = aue.select("span.hlFld-ContribAuthor > a#authors").first();
					if (author_name_element != null) {
						author_name = author_name_element.text().trim();
					} else {
						author_name = aue.select("span.hlFld-ContribAuthor").first().text().trim();
					}

					if (author_name.equals("") || author_name.equals("†") || author_name.equals("‡")
							|| author_name.equals("§") || author_name.equals("*") || author_name.equals("⊥")
							|| author_name.equals("#") || author_name.equals("∥") || author_name.equals("∇")
							|| author_name.equals("¶") || author_name.equals("□") || author_name.equals("○")
							|| author_name.equals("◆") || author_name.equals("∞") || author_name.equals("‖")) {
						// 跳过一些特殊字符(例如https://pubs.acs.org/doi/10.1021/ma050833f) 同时跳过作者为空的情况
						continue;
					}

					// 默认无上标
					String sup = "";
					String sup_f = "";
					String sup_s = "";
					Elements sup_first_condition = aue.select("span > span.NLM_xref-aff");
					Elements sup_second_condition = aue.select("span > a.ref");
					if (sup_first_condition.size() > 0) {
						for (Element sfc : sup_first_condition) {
							if (sfc.text().trim().equals("*")) {
								continue;
							}
							sup_f += sfc.text().trim() + ",";
						}
					}

					if (sup_second_condition.size() > 0) {
						for (Element ssc : sup_second_condition) {
							if (ssc.text().trim().equals("*")) {
								continue;
							}
							sup_s += ssc.text().trim() + ",";
						}
					}

					sup_f = sup_f.replaceAll(",+$", "");
					sup_s = sup_s.replaceAll(",+$", "");

					// sup_f和sup_s可能同时存在
					if (sup_f.equals("") && !sup_s.equals("")) {// sup_f为空 sup_s不为空
						sup = sup_s;
					} else if (!sup_f.equals("") && sup_s.equals("")) {// sup_f不为空 sup_s为空
						sup = sup_f;
					} else if (!sup_f.equals("") && !sup_s.equals("")) {// sup_f sup_s均不为空
						sup = sup_f + "," + sup_s;
					} else {
						sup = "";
					}

					if (!author_name.equals("")) { // 作者不为空才拼接上标
						if (!sup.equals("")) {
							author += author_name + "[" + sup + "]" + ";";
						} else {
							author += author_name + ";";
						}
					} else {
						author = "";
					}
				}

				if (author.equals("[]")) {
					author = "";
				} else {
					author = author.replaceAll(";+$", "");
				}
			}

			// organ 作者机构
			// 多种情况:无单位,只有一个单位(无上标或有上标),多个单位(无/上标/多上标),默认无单位
//			String organ = "";
			Element affiliation_element = doc.select("div.affiliations").first();
			if (affiliation_element != null) { // 有作者机构
//				System.out.println("***********begin**********");
				Elements aff_elements = affiliation_element.select("div[id^=aff]");
				if (aff_elements.size() > 0) {// 作者机构下面有aff标签
					for (Element afe : aff_elements) {// 每个aff标签下的作者机构取出来
						String afe_html = afe.html();
						String afe_institution = "";
//						System.out.println(afe_html);
//						System.out.println("***********aff**********");
						afe_html = afe_html.replaceAll("<sup>\\s*</sup>", ""); // 把aff下sup内容为空的值替换掉
						Document afe_doc = Jsoup.parse(afe_html);
						Elements aff_sup_elements = afe_doc.getElementsByTag("sup");// 获取所有的sup标签
						if (aff_sup_elements.size() > 0) {
							for (Element nse : aff_sup_elements) {// 替换sup: <sup>xxx</sup> ---> ;[xxx]
								String nes_text = nse.text().trim();
								if (!nes_text.matches("^[0-9]*$")) { // sup值不为数字时才替换
//									String sup_string = "<sup>(".concat(nes_text).concat(")</sup>");
									String sup_string = "<sup>([^<]+?)</sup>";
									afe_html = afe_html.replaceAll(sup_string, ";[$1]");
								}
							}
							afe_institution = Jsoup.parse(afe_html).text().trim();
						} else { // 没有sup标签
							afe_institution = afe_doc.text().trim();
						}
						organ += afe_institution;
					}
					organ = organ.replaceAll("^+;", "");
				} else { // 没有aff标签,只有div标签
//					System.out.println("***********div**********");
					Element div_element = affiliation_element.select("div").first();
					if (div_element != null) {
						String div_institution = "";
						String de_html = div_element.html();
						de_html = de_html.replaceAll("<sup>\\s*</sup>", "");
						Document de_doc = Jsoup.parse(de_html);
						Elements de_sup_elements = de_doc.getElementsByTag("sup");
						if (de_sup_elements.size() > 0) {
							for (Element dse : de_sup_elements) {
								String dse_text = dse.text().trim();
								if (!dse_text.matches("^[0-9]*$")) {
//									String dse_sup_string = "<sup>(" + dse_text + ")</sup>";
									String dse_sup_string = "<sup>([^<]+?)</sup>";
									de_html = de_html.replaceAll(dse_sup_string, ";[$1]");
								}
							}
							div_institution = Jsoup.parse(de_html).text().trim();
						} else {
							div_institution = de_doc.text().trim();
						}
						organ = div_institution.replaceAll("^+:", "");
					}
				}
//				System.out.println(creator_institution);
//				System.out.println("***********end**********");
			}

			// 期刊名
			// div#series-logo >div>a>img[@alt]
//			String journal_name = "";
			Element jtElement = doc.select("div.body-none >a >img").first();
			if (jtElement != null) {
				journal_name = jtElement.attr("alt").trim();
			}

			// 年 卷 期
			// ACS Appl. Bio Mater., 2018, 1 (3), pp 544–548
//			String pub_year = "";
//			String vol = "";
//			String num = "";
			Element dateElement = doc.select("div#citation").first();
			if (dateElement != null) {
				String citation_string = dateElement.text().trim();
				Element yearElement = dateElement.select("span.citation_year").first();
				Element volumeElement = dateElement.select("span.citation_volume").first();
				if (yearElement != null) {
					pub_year = yearElement.text().trim();
				} else {
					pub_year = citation_string.split(",")[1];
				}

				if (volumeElement != null) {
					vol = volumeElement.text().trim();
				} else {
					vol = citation_string.split(",")[2].split("\\(")[0];
				}

				if (citation_string.contains("(") && citation_string.contains(")")) {
					num = citation_string.split("\\(")[1].split("\\)")[0];
				} else {
					num = "";
				}
			}

			// abstracts 摘要
			// 头标签有就取头标签,没有就取full-text里的全部内容
//			String description = "";
			Element absElement = doc.select("meta[name=dc.Description]").first();
			if (absElement != null) {
				description = absElement.attr("content").trim();
				// 去除标签
				description = Jsoup.parse(description).text().trim();
			} else {
				Element absElement2 = doc.select("div.hlFld-Fulltext").first();
				if (absElement2 != null) {
					description = absElement2.text().trim();
				}
			}

			// 关键词
//			String keyword = "";
			Element kwElement = doc.select("meta[name=keywords]").first();
			if (kwElement != null) {
				keyword = kwElement.attr("content");
			}

			// beginpage endpage起始页
//			String page = "";
//			String beginpage = "";
//			String endpage = "";
			Element pageElement = doc.select("div#citation").first();
			if (pageElement != null) {
				// Anal. Chem., 1964, 36 (4), pp 127A–127A | J. Chem. Educ., 1954, 31 (7), p 380
				// | J. Chem. Educ., 1954, 31 (7),
				String citation_string = pageElement.text().trim();
				String[] citation_split = citation_string.split(",");
				String[] page_split = citation_split[citation_split.length - 1].split("p");
				page = page_split[page_split.length - 1].trim();

				if (page.contains("(") || page.contains(")")) {
					page = "";
				}

				if (!page.equals("") && page.split("–").length > 1) {
					begin_page = page.split("–")[0].trim();
					end_page = page.split("–")[1].trim();
				} else {
					begin_page = page;
					end_page = page;
				}
			}

			// 收稿日期 接受日期 修订日期 加工日期
//			recv_date = "";
//			accept_date = "";
//			revision_date = "";
//			pub_date_alt = "";
//			pub_date = "";
			Element date_element = doc.select("div.literatumContentItemHistory > div.wrapped >div.widget-body").first();
			if (date_element != null) {
				Elements date_div_elments = date_element.select("div");
				if (date_div_elments.size() > 0) {
					for (Element dde : date_div_elments) {
						String dde_string = dde.text().trim().toLowerCase();
						if (dde_string.contains("received")) {
							recv_date = dde_string.split("received")[1].trim();
							recv_date = DateTimeHelper.stdDate(recv_date);
						} else if (dde_string.contains("accepted")) {
							accept_date = dde_string.split("accepted")[1].trim();
							accept_date = DateTimeHelper.stdDate(accept_date);
						} else if (dde_string.contains("online")) {
							pub_date_alt = dde_string.split("online")[1].trim();
							pub_date_alt = DateTimeHelper.stdDate(pub_date_alt);
						} else if (dde_string.contains("print")) {
							pub_date = dde_string.split("print")[1].trim();
							pub_date = DateTimeHelper.stdDate(pub_date);
						}
					}
				}
			}
//			System.out.println("recv_date:" + recv_date);
//			System.out.println("accept_date:" + accept_date);
//			System.out.println("pub_date_alt:" + pub_date_alt);
//			System.out.println("pub_date:" + pub_date);

			// 出版方
//			String publisher = "";
			Element pubElement = doc.select("meta[name=dc.Publisher]").first();
			if (pubElement != null) {
				publisher = pubElement.attr("content").trim();
			}

			// 期刊网址
//			String provider_url = "";
			Element prourlElment = doc.select("meta[property=og:url]").first();
			if (prourlElment != null) {
				provider_url = prourlElment.attr("content").trim();
			}

			// 期刊名ID
			// issn 在article页面没有 取唯一标识符journal_id对应后再插入
//			String journal_id = "";
			Element navElement = doc.select("span.journalNavTitle > a").first();
			if (navElement != null) {
				String gch_url = navElement.attr("href").trim();
				String[] gchs = gch_url.split("/");
				journal_id = gchs[gchs.length - 3];
			}

			return true;
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			{
				rawid = "";
				down_date = "";
				doi = "";
				title = "";
				keyword = "";
				description = "";
				begin_page = "";
				end_page = "";
				raw_type = "";
				recv_date = "";
				accept_date = "";
				pub_date = "";
				pub_date_alt = "";
				author = "";
				organ = "";
				journal_id = "";
				journal_name = "";
				pub_year = "";
				vol = "";
				num = "";
				publisher = "";
				provider_url = "";
			}

			Gson gson = new Gson();
			Type type = new TypeToken<Map<String, String>>() {
			}.getType();
			Map<String, String> mapJson = gson.fromJson(value.toString(), type);

			String down_date = mapJson.get("down_date").trim();

			// rawid 文章唯一标识
			String rawid = mapJson.get("rawid").trim();
			String[] rawid_spilt = rawid.split("/");
			rawid = rawid_spilt[rawid_spilt.length - 1];

			String html = mapJson.get("html").trim();
			Document doc = Jsoup.parse(html);

			// 解析html
			parseHtml(doc);

			XXXXObject xObj = new XXXXObject();

			xObj.data.put("rawid", rawid);
			xObj.data.put("down_date", down_date);
			xObj.data.put("batch", batch);
			xObj.data.put("doi", doi);
			xObj.data.put("title", title);
			xObj.data.put("keyword", keyword);
			xObj.data.put("description", description);
			xObj.data.put("begin_page", begin_page);
			xObj.data.put("end_page", end_page);
			xObj.data.put("raw_type", raw_type);
			xObj.data.put("recv_date", recv_date);
			xObj.data.put("accept_date", accept_date);
			xObj.data.put("pub_date", pub_date);
			xObj.data.put("pub_date_alt", pub_date_alt);
			xObj.data.put("author", author);
			xObj.data.put("organ", organ);
			xObj.data.put("journal_id", journal_id);
			xObj.data.put("journal_name", journal_name);
			xObj.data.put("pub_year", pub_year);
			xObj.data.put("vol", vol);
			xObj.data.put("num", num);
			xObj.data.put("publisher", publisher);
			xObj.data.put("provider_url", provider_url);

			xObj.data.put("page", page);

			context.getCounter("map", "count").increment(1);

			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(rawid), new BytesWritable(bytes));

		}

	}
}