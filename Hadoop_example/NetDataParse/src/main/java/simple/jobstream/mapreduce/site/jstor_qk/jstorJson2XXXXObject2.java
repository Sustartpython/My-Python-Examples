package simple.jobstream.mapreduce.site.jstor_qk;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.security.acl.Group;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.print.DocFlavor.STRING;

import org.apache.avro.JsonProperties.Null;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.datanode.dataNodeHome_jsp;
import org.apache.hadoop.hdfs.server.namenode.status_jsp;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.xerces.impl.dv.xs.DayDV;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;
import org.jsoup.select.Elements;
import org.mockito.internal.matchers.And;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class jstorJson2XXXXObject2 extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 20;
	private static int reduceNum = 20;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = ""; // 这个目录会被删除重建

	public void pre(Job job) {
		String jobName = this.getClass().getSimpleName();
		if (testRun) {
			jobName = "test_" + jobName;
		}
		job.setJobName("jstor." + jobName);

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
		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.7f);
		System.out.println("******mapred.reduce.slowstart.completed.maps*******"
				+ job.getConfiguration().get("mapred.reduce.slowstart.completed.maps"));
		job.getConfiguration().set("io.compression.codecs",
				"org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(ProcessReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);

		// job.setInputFormatClass(SimpleTextInputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		SequenceFileOutputFormat.setCompressOutput(job, false);
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

	public static class ProcessMapper extends Mapper<LongWritable, Text, Text, BytesWritable> {

		static int cnt = 0;
//		public BufferedWriter out = null;
//		public FSDataOutputStream fout = null;

		// 清理的分号和空白
		static String cleanLastSemicolon(String text) {
			text = text.replace('；', ';'); // 全角转半角
			text = text.replaceAll("\\s*;\\s*", ";"); // 去掉分号前后的空白
			text = text.replaceAll("\\s*\\[\\s*", "["); // 去掉[前后的空白
			text = text.replaceAll("\\s*\\]\\s*", "]"); // 去掉]前后的空白
			text = text.replaceAll("[\\s;]+$", ""); // 去掉最后多余的空白和分号

			return text;
		}

		Map<String, String> monthMap = new HashMap<String, String>() {
			{
				put("january", "01");
				put("february", "02");
				put("februaryy", "02");
				put("march", "03");
				put("april", "04");
				put("may", "05");
				put("june", "06");
				put("july", "07");
				put("august", "08");
				put("september", "09");
				put("october", "10");
				put("november", "11");
				put("december", "12");
				put("jan", "01");
				put("feb", "02");
				put("mar", "03");
				put("apr", "04");
				put("jun", "06");
				put("jul", "07");
				put("aug", "08");
				put("sept", "09");
				put("sep", "09");
				put("oct", "10");
				put("nov", "11");
				put("dec", "12");
			}
		};

		// 清理space，比如带大括号的情况（xiandaijj201204178:{G445}）
		static String cleanSpace(String text) {
			text = text.replaceAll("[\\s\\p{Zs}]+", " ").trim();
			return text;
		}

		// 国家class
		static String getCountrybyString(String text) {
			Dictionary<String, String> hashTable = new Hashtable<String, String>();
			hashTable.put("中国", "CN");
			hashTable.put("英国", "UK");
			hashTable.put("日本", "JP");
			hashTable.put("美国", "US");
			hashTable.put("法国", "FR");
			hashTable.put("德国", "DE");
			hashTable.put("韩国", "KR");
			hashTable.put("国际", "UN");

			if (null != hashTable.get(text)) {
				text = hashTable.get(text);
			} else {
				text = "UN";
			}

			return text;
		}

		// 语言class
		static String getLanguagebyCountry(String text) {
			Dictionary<String, String> hashTable = new Hashtable<String, String>();
			hashTable.put("CN", "ZH");
			hashTable.put("UK", "EN");
			hashTable.put("US", "EN");
			hashTable.put("JP", "JA");
			hashTable.put("FR", "FR");
			hashTable.put("DE", "DE");
			hashTable.put("UN", "UN");
			hashTable.put("KR", "KR");
			text = hashTable.get(text);

			return text;
		}

		public boolean log2HDFSForMapper(Context context, String text) {
		   Date dt=new Date();//如果不需要格式,可直接用dt,dt就是当前系统时间
		   DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");//设置显示格式
		   String nowTime = df.format(dt);//用DateFormat的format()方法在dt中获取并以yyyy/MM/dd HH:mm:ss格式显示
		   
		   df = new SimpleDateFormat("yyyyMMdd");//设置显示格式
		   String nowDate = df.format(dt);//用DateFormat的format()方法在dt中获取并以yyyy/MM/dd HH:mm:ss格式显示
		   
		   text = nowTime + "\n" + text + "\n\n";
		   
		   boolean bException = false;

		   try {
		    // 获取HDFS文件系统  
		          FileSystem fs = FileSystem.get(context.getConfiguration());
		    
		          FSDataOutputStream fout = null;
		          String pathfile = "/user/xujiang/logs/logs_map_jstor/" + nowDate + ".txt";
		          if (fs.exists(new Path(pathfile))) {
		           fout = fs.append(new Path(pathfile));
		          } else {
		           fout = fs.create(new Path(pathfile));
		          }
		       
		          
		       BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fout, "UTF-8"));
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

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			context.getCounter("map", "inputcount").increment(1);

			String text = value.toString().trim();

			Gson gson = new Gson();
			Type type = new TypeToken<Map<String, Object>>() {
			}.getType();

			Map<String, Object> mapField = gson.fromJson(text, type);

			if (mapField.get("url") == null) {
				context.getCounter("map", "url is null").increment(1);
				return;
			}

			if (mapField.get("html") == null) {
				context.getCounter("map", "html is null").increment(1);
				return;
			}

			String url = mapField.get("url").toString();
			String html = mapField.get("html").toString();
			String teString = html.replace("\0", " ").replace("\r", " ").replace("\n", " ") + "\n";
			html = teString;

			if (html.length() < 10) {
				//表示html不完整放弃
				log2HDFSForMapper(context, "null html:" + url);
				return;
			}
			context.getCounter("map", "havehtmlcount").increment(1);
			Document doc = Jsoup.parse(html);

			String id = "";
			String rawid = "";
			// 关键字
			String keywords = "";
			// 摘要
			String abstracts = "";
			// 出版社
			String contentPublisher = "";
			// 分类
			String contentDiscipline = "";
			// 标题
			String title = "";
			// issn
			String issn = "";
			// eissn
			String eissn = "";
			// 页面
			String page = "";
			// doi
			String doi = "";
			// 期刊名
			String source = "";
			// issue
			String issuenumber = "";
			// vol
			String vol = "";
			// year
			String date = "1900";
			//
			String date_created = "1900000";
			// 国家
			String country = "";
			// 语言
			String language = "";
			// 月
			String month = "";
			// 日
			String day = "";

			String pageCount = "";
			// 作者
			String author = "";
			String gch = "";

			country = getCountrybyString("美国");
			language = getLanguagebyCountry(country);

			Elements eissnElementst = doc.getElementsByAttributeValue("class", "eissn mtm");
			if (eissnElementst.isEmpty()) {
				eissn = "";

			} else {
				eissnElementst = doc.getElementsByAttributeValue("class", "eissn mtm");
				eissn = eissnElementst.first().text().trim();
				eissn = eissn.replace("EISSN:", "").trim();
				if (eissn.length() == 8) {
					StringBuilder sb = new StringBuilder();
					sb.append(eissn).insert(4, "-");
					eissn = sb.toString();
				}
			}

			String pattern = "gaData.content = \\{(.*)?\\};  gaData.contentAccess";
			Pattern r = Pattern.compile(pattern);
			Matcher m = r.matcher(html);
			if (m.find()) {
				JsonObject jsondata = null;
				try {
					jsondata = new JsonParser().parse("{" + m.group(1) + "}").getAsJsonObject();
				} catch (Exception e) {
					// TODO: handle exception
					log2HDFSForMapper(context, "json err:" + url + ":" + m.group(1));
				}
				if (jsondata == null) {
					log2HDFSForMapper(context, "jsondata null:" + url);
					return;
				}
				source = jsondata.get("contentName").toString().replace("\"", "");
				doi = jsondata.get("objectDOI").toString().replace("\"", "");
				pageCount = jsondata.get("pageCount").toString().replace("\"", "");

				String contentIssue = jsondata.get("contentIssue").toString();
				if (!contentIssue.equals("")) {
					// 解析卷
					vol = "";
					if (contentIssue.indexOf("Vol") != -1) {
						pattern  = "Vol. (\\S+)";
						r = Pattern.compile(pattern);
						m = r.matcher(contentIssue);
						if (m.find()) {
							vol = m.group(1).toString().trim();
						    vol =  vol.replace("&amp;lrm;","").replace(",","");
						}
					}
					// 解析期
					if (contentIssue.indexOf("No.") != -1) {
						pattern = "No. (\\S+)";
						r = Pattern.compile(pattern);
						m = r.matcher(contentIssue);
						issuenumber = "";
						if (m.find()) {
							try {
								issuenumber = m.group(1).toString().trim();
								issuenumber =  issuenumber.replace("&amp;lrm;","").replace(",","");
							} catch (Exception e) {
								//其他情况
//								pattern = "No. (\\S+)?,";
//								r = Pattern.compile(pattern);
//								m = r.matcher(contentIssue);
//								if (m.find()) {
//									try {
//										issuenumber = m.group(1).toString().trim();
//										
//									} catch (Exception e1) {
//										log2HDFSForMapper(context, "必须解决的错误issuenumber:" + url + ":" + contentIssue);
//									}
//								}
									
							}

						}
					}
					// 解析括号中的数据
					if (contentIssue.indexOf("(") != -1) {
						pattern = "\\((.*)?\\)(.*)?";
						r = Pattern.compile(pattern);
						m = r.matcher(contentIssue);
						if (m.find()) {
							// 页
							page = m.group(2).replace(",", "").trim();
							if (page != null && !page.equals("")) {
								String[] pages = page.split(" ");
								page = pages[pages.length - 1].trim().replace("\"", "");
							} else {
								page = "";
							}
							// 括号中的数据
							String dates = m.group(1).toString().trim();
							if (dates != null) {
								dates = dates.replace(",", "").replace(".", "").trim();
								pattern = "(.*)?(\\d{4})";
								r = Pattern.compile(pattern);
								m = r.matcher(dates);
								if (m.find()) {
									String months = m.group(1);
									// 年
									date = m.group(2);
									if (date == null) {
										date = "1900";
									} else {
										date = date.trim();
									}
									// 月和日
									pattern = "(\\D+)?(\\s)?(\\d{1,2})?[\\s,]";
									r = Pattern.compile(pattern);
									m = r.matcher(months);
									if (m.find()) {
										month = m.group(1);
										day = m.group(3);
										if (month == null) {
											month = "00";
										} else {
											month = month.trim();
										}
										if (day == null) {
											day = "00";
										} else {
											day = day.trim();
										}

									}

								} else {
									log2HDFSForMapper(context, "没有匹配到括号里的数据:" + url);
								}

							}
						} else {
							issuenumber = "";
						}
					} else {
						issuenumber = "";
					}
				} else {
					log2HDFSForMapper(context, "contentIssue没有:" + url);
				}
				// 出版
				contentPublisher = jsondata.get("contentPublisher").toString().replace("\"", "");
				// 分类
				contentDiscipline = jsondata.get("contentDiscipline").toString().replace("\"", "");
				contentDiscipline = contentDiscipline.replace("[", "").replace("]", "").replace("&#39;", "")
						.replace(", ", ";");
//	            System.out.println(contentDiscipline);
				// 标题
				title = jsondata.get("itemTitle").toString().replace("\"", "");
				title = title.trim();
				if (title == null || title == "" || title.length() < 1) {
					Elements eissnElementst1 = doc.getElementsByAttributeValue("class", "rw mbs");
					if (!eissnElementst1.isEmpty()) {
						title = eissnElementst1.first().text().toString().trim();
					}
				}
				if (title == null || title == "" || title.length() < 1) {
					Elements eissnElementst1 = doc.getElementsByAttributeValue("class", "medium-heading title");
					if (!eissnElementst1.isEmpty()) {
						title = eissnElementst1.first().text().toString().trim();
					}
				}
				if (title == null || title == "" || title.length() < 1) {
					Elements eissnElementst1 = doc.getElementsByAttributeValue("class",
							"mtm breadcrumb-article current");
					if (!eissnElementst1.isEmpty()) {
						title = eissnElementst1.first().text().toString().trim();
					}
				}
				if (title == null || title == "" || title.length() < 1) {
					throw new InterruptedException("title is null"+ url);
				}
			} else {
				context.getCounter("map", "nogaData").increment(1);
				log2HDFSForMapper(context, "**nogaData**" + url);
				return;
			}

			try {
				Elements issnElements = doc.getElementsByAttributeValue("class", "issn mtm");
				if (issnElements.isEmpty()) {
					issn = "";
				} else {
					issn = issnElements.first().text().trim();
					issn = issn.replace("ISSN:", "").trim();
					if (issn.length() == 8) {
						StringBuilder sb = new StringBuilder();
						sb.append(issn).insert(4, "-");
						issn = sb.toString();
					}
				}
				
			} catch (Exception e) {
				// TODO: handle exception
				log2HDFSForMapper(context, "**issn**" + url + ":" + e.getMessage());

			}

			Element aElement = doc.select("ul > li.breadcrumb-journal > a").first();
			if (aElement != null) {
				String href = aElement.attr("href");
				gch = href.split("\\?")[1].split("=")[1].trim();
			}else {
				throw new InterruptedException("gch is null:"+ url);
			}

			try {
				Elements keyworlds_input = doc.select("#thumbs-up-form > input[name=\"topic\"]");
				keywords = keyworlds_input.first().attr("value");
			} catch (Exception e) {
				keywords = "";
			}

			try {
				Elements abstract_Elements = doc.select("div.abstract1");
				abstracts = abstract_Elements.first().text().trim();
			} catch (Exception e) {
				abstracts = "";
			}

			Element eleAuthor = doc.select("input[name=item_authors]").first();

			if (eleAuthor != null) {
				author = eleAuthor.attr("value").trim();

			} else {
				author = "";
			}

//			************************日期处理********************************
			if (month == null || month.equals("")) {
				month = "00";
				date_created = date + "0000";
			} else {
				month = month.trim().toLowerCase().replace(".", "").replace(",", "");
				month = monthMap.get(month);
				if (month == null || month.equals("")) {
					month = "00";
				}
				if (day == null || day.equals("")) {
					date_created = date + month + "00";
				} else {
					if (day.trim().length() == 1) {
						day = "0" + day;
					}
					date_created = date + month + day;
				}
			}

//           ************************************************************
			id = url.replace("/stable/", "");
			url = "https://www.jstor.org" + url;
			rawid = id;

			XXXXObject xObj = new XXXXObject();
			xObj.data.put("rawid", rawid);
			xObj.data.put("id", id);
			xObj.data.put("subject", keywords);
			xObj.data.put("description", abstracts);
			xObj.data.put("publisher", contentPublisher);
			xObj.data.put("provider_subject", contentDiscipline);
			xObj.data.put("title", title);
			xObj.data.put("identifier_pissn", issn);
			xObj.data.put("identifier_eissn", eissn);
			xObj.data.put("page", page);
			xObj.data.put("identifier_doi", doi);
			xObj.data.put("source", source);
			xObj.data.put("issue", issuenumber);
			xObj.data.put("volume", vol);
			xObj.data.put("date", date);
			xObj.data.put("date_created", date_created);
			xObj.data.put("country", country);
			xObj.data.put("language", language);
			xObj.data.put("url", url);
			xObj.data.put("pageCount", pageCount);
			xObj.data.put("author", author);
			xObj.data.put("gch", gch);

			context.getCounter("map", "count").increment(1);

			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(rawid), new BytesWritable(bytes));	

		}
	}

	public static class ProcessReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
		public void reduce(Text key, Iterable<BytesWritable> values, Context context)
				throws IOException, InterruptedException {

			BytesWritable bOut = new BytesWritable(); // 用于最后输出
			for (BytesWritable item : values) {
				if (item.getLength() > bOut.getLength()) { // 选最大的一个
					bOut.set(item.getBytes(), 0, item.getLength());
				}
			}

			context.getCounter("reduce", "count").increment(1);

			bOut.setCapacity(bOut.getLength()); // 将buffer设为实际长度

			context.write(key, bOut);
		}
	}
}
