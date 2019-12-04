package simple.jobstream.mapreduce.site.sagejournal;

import java.io.IOException;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
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
//import org.apache.tools.ant.taskdefs.Length;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

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

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class Jsonxxxxobject extends InHdfsOutHdfsJobInfo {

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

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		JobConfUtil.setTaskPerMapMemory(job, 1024 * 10);
		JobConfUtil.setTaskPerReduceMemory(job, 1024 * 8);
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

		private static String logHDFSFile = "/user/qianjun/log/log_map/" + DateTimeHelper.getNowDate() + ".txt";
		static int cnt = 0;

		private static String rawid = "";
		private static String identifier_doi = "";
		private static String title = "";
		private static String identifier_pissn = "";
		private static String identifier_eissn = "";
		private static String creator = "";
		private static String source = "";
		private static String creator_institution = "";
		private static String date = "";
		private static String volume = "";
		private static String issue = "";
		private static String description = "";
		private static String page = "";
		private static String subject = "";
		private static String cited_cnt = "";
		private static String date_created = "";
		private static String journalId = "";
		private static String corr_author = "";
		private static String publisher = "";
		private static String down_date = "";
		private static String batch = "";

		public void setup(Context context) throws IOException, InterruptedException {
			batch = context.getConfiguration().get("batch");
			context.getCounter("batch", batch).increment(1);
		}

		public void parseHtml(String htmlText) {

			Document doc = Jsoup.parse(htmlText);

			Element dateElement = doc.select("span[class=publicationContentEpubDate dates]").first();
			if (dateElement != null) {

				String deal_date = dateElement.text().trim().replace("First Published ", "");

				date_created = DateTimeHelper.stdDate(deal_date);

			}

			// 获取出版社
			Element publisherElement = doc.select("meta[name =dc.Publisher]").first();

			if (publisherElement != null) {
				publisher = publisherElement.attr("content").replace("SAGE Publications", "").trim();

			}

			// 处理beginpage,endpage

			Element subjectElement = doc.select("div[class=hlFld-KeywordText]").first();
			if (subjectElement != null) {

				subject = subjectElement.text().trim().replace("Keywords ", "").replace(",", ";");
			}

			Element descriptionElement = doc.select("div[class=abstractSection abstractInFull]").first();
			if (descriptionElement != null) {

				description = descriptionElement.text().trim().replace("Keywords ", "").replace(",", ";");
			}

			Element doiElement = doc.select("a[class=doiWidgetLink]").first();
			if (doiElement != null) {

				identifier_doi = doiElement.text().trim().replace("https://doi.org/", "").replace("http://doi.org/",
						"");

			}

			Element cited_cntElement = doc.select("div[class=citing-label]").first();
			if (cited_cntElement != null) {

				String deal_cited_cnt = cited_cntElement.text().trim().replace("Citing articles: ", "");
				Pattern pattern = Pattern.compile("[^0-9]");
				Matcher matcher = pattern.matcher(deal_cited_cnt);
				String last_cited_cnt = matcher.replaceAll("");
				cited_cnt = last_cited_cnt;
			}
			// 获取通讯作者
			Element corr_authorElement = doc.select("div[class='artice-notes']").first();

			if (corr_authorElement != null) {

				// 获取该标签下，所有的a标签，如果有多个a标签则是有多个通讯作者

				Elements corr_author_aElement = corr_authorElement.select("a[class='email']");

				for (Element corr_author_one : corr_author_aElement) {

					// 获取herf属性
					String corr_author_href = corr_author_one.attr("href");

					if (corr_author_href.contains("@")) {

						String corr_author_deal = corr_author_href.split("@")[0].replace("mailto:", "")
								.replace(".cn", "").replace(".com", "").trim();
						corr_author = corr_author + corr_author_deal + ";";

					}

				}

			}
			if (corr_author.length() != 0 && corr_author.endsWith(";")) {
				corr_author = corr_author.substring(0, corr_author.length() - 1);

			}
			// 处理作者机构
			Element creatorElement = doc.select("div[class=author name]").first();

			if (creatorElement != null) {
				LinkedHashMap params = new LinkedHashMap();
				// 通过获取span class="NLM_contrib-group"标签，如果含有多个则是case6情况

				Elements group_caseElement = doc.select("span[class='NLM_contrib-group']");

				if (group_caseElement.size() != 1) {

					// case6 一个标签中含有多个span[class='NLM_contrib-group']，且作者机构一一包含
					for (Element case1 : group_caseElement) {

						// 获取所有作者
						Elements creatorsElement = case1.select("span[class='contribDegrees']");

						if (creatorsElement.size() != 0) {

							for (Element case1_creator : creatorsElement) {

								// 获取作者标签
								Element entryAuthorElement = case1_creator.select("a[class='entryAuthor']").first();
								String organ = "";
								if (entryAuthorElement != null) {
									// 获取作者
									String author = entryAuthorElement.text().trim();

									// 获取link标签中的机构标签
									Elements organsElement = case1.select("div[class='artice-info-affiliation']");

									if (organsElement.size() != 0) {
										for (Element organ_case1 : organsElement) {

											// 获取机构字符串
											String organ_case1_temp = organ_case1.text().trim();

											organ = organ + organ_case1_temp + ";";

										}
//	                                             System.out.println(organ);
										params.put(author, organ);
									}

									else {
										params.put(author, "");
									}
								}
							}
						}
					}
				} else {

					// 获取作者与机构对应编号的标签
					Element affnumEelment = creatorElement.select("span[class='NLM_xref-aff']").first();

					if (affnumEelment != null) {
						// 则说明是case1，作者与机构有相对应的编号

						// 获取所有作者
						Elements authorsElement = creatorElement.select("span[class='contribDegrees']");
						String organ = "";
						for (Element author : authorsElement) {

							// 获取作者字符串
							Element author_case1Element = author.select("a[class ='entryAuthor']").first();

							if (author_case1Element != null) {

								String author_str = author_case1Element.text().trim();
								// 获取机构编号标签
								Elements organnumElement = author.select("span[class='NLM_xref-aff']");

								if (organnumElement.size() != 0) {
									String organ_case1 = "";
									for (Element organnum : organnumElement) {

										// 获取机构编号

										String organ_num = organnum.text().trim();

										// 获取所有机构机构
										Elements allorganElement = creatorElement
												.select("div[class=artice-info-affiliation]");

										for (Element organ_case1Element : allorganElement) {

											String organ_case1_str = organ_case1Element.text().trim();

											if (organ_case1_str.startsWith(organ_num)) {
												String deal_organ_str = organ_case1_str.replace(organ_num, "");

												organ_case1 = organ_case1 + deal_organ_str + ";";
											}
										}
									}
									params.put(author_str, organ_case1);

								} else {
									params.put(author_str, "");
								}
							}
						}
					}

					// 获取div 作者标签

					Elements authordivElement = creatorElement.select("div[class='contribDegrees']");

					// span作者标签
					Elements authorspanElement = creatorElement.select("span[class='contribDegrees']");

					// 获取机构标签
					Elements organtagElement = creatorElement.select("div[class='artice-info-affiliation']");

					// 当只有作者，没有作者span标签，没有机构标签，则是case2,只有作者，无机构
					if (authordivElement.size() == 0 && authorspanElement.size() != 0 && organtagElement.size() == 0) {

						for (Element authorspan : authorspanElement) {

							String authorspan_str = authorspan.text().trim();
							params.put(authorspan_str, "");
						}
					}

					// case3
					else if (authordivElement.size() != 0 && authorspanElement.size() != 0
							&& organtagElement.size() != 0) {

						// 获取所有作者标签
						Elements author_case3Element = creatorElement.select("[class='contribDegrees']");

						for (Element author_case3 : author_case3Element) {

							// 获取作者，拿到当前标签下作者
							Element author_case3_str = author_case3.select("a[class='entryAuthor']").first();

							if (author_case3_str == null) {
								continue;
							}
							String author_str = author_case3_str.text().trim();

							// 获取该标签下，是否有机构
							Elements organ_case3Element = author_case3.select("div[class='artice-info-affiliation']");

							String organ_case3_str = "";
							if (organ_case3Element.size() != 0) {
								for (Element organ_case3 : organ_case3Element) {

									// 获取机构字符串
									String organ_case3_temp = author_case3.text().trim();

									organ_case3_str = organ_case3_str + organ_case3_temp + ";";

								}

								params.put(author_str, organ_case3_str);

							} else {
								params.put(author_str, "");
							}

						}

					} else if (authordivElement.size() == 0 && authorspanElement.size() != 0
							&& organtagElement.size() != 0) {

						Element case4_5_groupElement = creatorElement.select("span[class='NLM_contrib-group']").first();
						Element affnum_case1Eelment = creatorElement.select("span[class='NLM_xref-aff']").first();

						if (case4_5_groupElement != null && affnum_case1Eelment == null) {

							// 获取当前标签下，所有作者名字的标签
							Elements all_author_case4_5Element = case4_5_groupElement
									.select("span[class='contribDegrees']");

							// 根据作者机构是否在同一个标签中，区分case4,case5两种情况
							Elements organ_case4Element = case4_5_groupElement
									.select("div[class='artice-info-affiliation']");

							if (organ_case4Element.size() == 1) {
								// 多个作者对应一个标签的情况，且作者机构在同一个标签中

								String organ_case4 = "";
								// 遍历所有作者，进行拼接
								for (Element case4_author : all_author_case4_5Element) {

									// 获取机构字符串
									String author_case4_temp = case4_author.text().trim();

									// 获取机构
									organ_case4 = organ_case4Element.text().trim();
									params.put(author_case4_temp, organ_case4);
								}
							}

							else if (organ_case4Element.size() == 0) {

								// 作者机构不在同一个标签中，需要自行一一对应
								// 获取当前标签下，所有作者名字的标签
								Elements all_author_case5Element = case4_5_groupElement
										.select("span[class='contribDegrees']");

								// 获取当前作者的所有标签
								Elements all_organ_case5Element = creatorElement
										.select("div[class='artice-info-affiliation']");

								if (all_author_case5Element.size() == all_organ_case5Element.size()) {

									// 一一对应拿取
									int author_num = 0;

									for (Element case5_author : all_author_case5Element) {
										author_num += 1;
										int organ_num = 0;
										// 获取作者
										String case5_author_str = case5_author.text().trim();
										for (Element case5_organ : all_organ_case5Element) {

											organ_num += 1;

											String case5_organ_str = case5_organ.text().trim();

											if (author_num == organ_num) {
												params.put(case5_author_str, case5_organ_str);
											}
										}
									}
								} else if (all_author_case5Element.size() != all_organ_case5Element.size()
										&& all_organ_case5Element.size() == 1) {

									// 这是说明，在csae6的情况下，出现作者机构不在同一个标签中，且无法一一对应，存在多个对一个
									// 多个作者对应一个标签的情况，且作者机构在不在同一个标签中

									Element organ_case7Element = creatorElement
											.select("div[class='artice-info-affiliation']").first();
									String organ_case7 = "";
									// 遍历所有作者，进行拼接
									for (Element case7_author : all_author_case4_5Element) {

										// 获取机构字符串
										String author_case7_temp = case7_author.text().trim();

										// 获取机构

										organ_case7 = organ_case7Element.text().trim();
										params.put(author_case7_temp, organ_case7);

									}

								}

							}
						}
					}
				}

				String[] AAA = AuthorOrgan.numberByMap(params);
				creator = AAA[0];
				creator_institution = AAA[1];

			}
			if ("".equals(creator) && "".equals(creator_institution)) {
				// 作者机构，都不编号的情况
				System.out.println("**************case8**************");
				System.out.println("当前解析rawid:" + rawid);
				Element creatorCase8Element = doc.select("div[class=author name]").first();

				if (creatorElement != null) {

					// 获取所有作者
					Elements authorsElement = creatorElement.select("[class=contribDegrees]");

					for (Element author : authorsElement) {

						String author_str = author.text().trim().replace(",", "");
						creator = creator + author_str + ";";

					}

					// 获取所有机构
					Elements organsElement = creatorElement.select("div[class='artice-info-affiliation']");

					for (Element organ : organsElement) {

						String organ_str = organ.text().trim();
						creator_institution = creator_institution + organ_str + ";";
					}
					creator = creator.replaceAll(";$", "").replace(" Ph.D.", "").replace("*", "")
							.replace(" M.D.", "").trim();
					;
					creator_institution = creator_institution.replaceAll(";$", "");

				}

			}

		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			{
				rawid = "";
				identifier_doi = "";
				title = "";
				identifier_pissn = "";
				identifier_eissn = "";
				creator = "";
				source = "";
				creator_institution = "";
				date = "";
				volume = "";
				issue = "";
				description = "";
				page = "";
				subject = "";
				cited_cnt = "";
				date_created = "";
				journalId = "";
				corr_author = "";
				publisher = "";
				down_date = "20190224";	 // 因为有些bigjson中为设置dwon_date

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

			rawid = mapField.get("doi").toString();
			title = mapField.get("title").toString();
			page = mapField.get("page").toString();
			// creators = mapField.get("creator").toString();
			volume = mapField.get("volume").toString();
			issue = mapField.get("issue").toString();
			journalId = mapField.get("jid").toString();
//			date = mapField.get("year").toString();
			// mouth = mapField.get("mouth").toString();

			source = mapField.get("jname").toString();
			identifier_pissn = mapField.get("eissn").toString();
			if (mapField.containsKey("Online_issn")) {
				identifier_eissn = mapField.get("Online_issn").toString();
			}else {
				identifier_eissn="";
			}
			
			// issue_id = mapField.get("issue_id").toString();
			String htmlText = mapField.get("html").toString();

			if (mapField.containsKey("down_date")) {
				down_date = mapField.get("down_date").toString();
			}

			if (!htmlText.toLowerCase().contains("widget-body body body-none  body-compact-all")) {
				context.getCounter("map", "Error: widget-body body body-none  body-compact-all").increment(1);
				return;
			}
		

			parseHtml(htmlText);
			if(date_created.equals("")) {
				date_created="19000000";
				
			}
			
			date = date_created.substring(0, 4);
		

			if (title.length() < 1) {
				context.getCounter("map", "Error: no title").increment(1);
				LogMR.log2HDFS4Mapper(context, logHDFSFile, "error: no title: " + rawid);
				return;
			}
			
			XXXXObject xObj = new XXXXObject();
			xObj.data.put("rawid", rawid);
			xObj.data.put("title", title);
			xObj.data.put("identifier_doi", identifier_doi);
			xObj.data.put("creator", creator);
			xObj.data.put("date", date);
			xObj.data.put("source", source);
			xObj.data.put("volume", volume);
			xObj.data.put("issue", issue);
			xObj.data.put("identifier_eissn", identifier_eissn);
			xObj.data.put("date_created", date_created);
			xObj.data.put("description", description);
			xObj.data.put("journalId", journalId);
			xObj.data.put("page", page);
			xObj.data.put("cited_cnt", cited_cnt);
			xObj.data.put("subject", subject);
			xObj.data.put("identifier_pissn", identifier_pissn);
			xObj.data.put("creator_institution", creator_institution);
			xObj.data.put("corr_author", corr_author);
			xObj.data.put("publisher", publisher);
			xObj.data.put("down_date", down_date);

			xObj.data.put("batch", batch);
			byte[] bytes = com.process.frame.util.VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(rawid), new BytesWritable(bytes));
			context.getCounter("map", "count").increment(1);

		}
	}
}
