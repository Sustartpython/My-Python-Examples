package simple.jobstream.mapreduce.site.jstor_book;

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
import org.apache.hadoop.hbase.generated.thrift.thrift_jsp;
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

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.util.StringHelper;
import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class Json2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 20;
	private static int reduceNum = 20;

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
	   	public static String batch = "";
	   	
		protected void setup(Context context) throws IOException, InterruptedException {
			batch = context.getConfiguration().get("batch");
		}

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
			if (text.equals("")) {
				return ;
			}

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
			String down_date = mapField.get("downdate").toString();
			String html = mapField.get("html").toString();
			String bookname = mapField.get("bookname").toString();
			String teString = html.replace("\0", " ").replace("\r", " ").replace("\n", " ") + "\n";
			html = teString;

			if (html.length() < 10) {
				//表示html不完整放弃
				log2HDFSForMapper(context, "null html:" + url);
				return;
			}
			context.getCounter("map", "havehtmlcount").increment(1);
			Document doc = Jsoup.parse(html);

			String rawid = "";
			// 摘要
			String abstracts = "";
			// 出版社
			String contentPublisher = "";
			// 分类
			String subject = "";
			// 标题
			String title = "";
			// eisbn
			String eisbn = "";
			// 页面
			String page = "";
			// doi
			String doi = "";
			// year
			String date = "1900";
			//
			String date_created = "1900000";
			// 国家
			String country = "";
			// 语言
			String language = "";
			// 作者
			String author = "";
			//系列
			String seriesstring = "";
			//图片
			String coverbase64 = ""; 
			//版本
			String edition = "";

			country = getCountrybyString("美国");
			language = getLanguagebyCountry(country);

			eisbn = "";
			Elements eissnElementst = doc.getElementsByAttributeValue("class", "book-eisbn mtm");
			if (!eissnElementst.isEmpty()) {
				eisbn = eissnElementst.first().text().trim();
				eisbn = eisbn.replace("eISBN:", "").replace("\"", "").trim();
			}

			String pattern = "gaData.content = \\{(.*)?\\};                dataLayer.push";
			Pattern r = Pattern.compile(pattern);
			Matcher m = r.matcher(html);
			if (m.find()) {
				JsonObject jsondata = null;
				try {
					jsondata = new JsonParser().parse("{" + m.group(1) + "}").getAsJsonObject();
				} catch (Exception e) {
					throw new InterruptedException("json err:" + url + ":" + m.group(1));
				}
				contentPublisher = jsondata.get("contentPublisher").toString().replace("\"", "");
			    contentPublisher = contentPublisher.replaceAll(",\\s?", ";").trim();
				doi = jsondata.get("objectDOI").toString().replace("\"", "");
				subject = jsondata.get("contentDiscipline").toString().replace("\"", "");
				subject = subject.replace("[", "").replace("]", "").replace("&#39;", "");
				subject = Jsoup.parse(subject).text();
				subject = subject.replaceAll(",\\s?", ";").trim();

			} else {
				log2HDFSForMapper(context, "contentIssue没有:" + url);
			}
			title = bookname.trim();
			if (title == null || title == "" || title.length() < 1) {
				throw new InterruptedException("title err:" + url);
			}
			
			String CopyrightDate = doc.select("div.published_date").first().text().trim();
			CopyrightDate = CopyrightDate.replace("Copyright Date:", "").trim();
			if (DateTimeHelper.checkYearByRange(CopyrightDate, 1000, 3000)) {
				date = CopyrightDate;
				date_created = date+"0000";
			}
			// 无pages /stable/10.1163/j.ctt1w76tx8
			Elements pages = doc.select("div.count");
			if (!pages.isEmpty()){
				page = pages.first().text().replace("Pages:", "").trim();
			}
			
			//有版本信息
			Elements editions = doc.select("div.book-edition");
			if (!editions.isEmpty()){
				edition = editions.first().text().replace("Edition:", "").trim();
			}
			

			
			abstracts = doc.select("div.book-description").first().text().trim();
			abstracts = abstracts.replace("Book Description:", "");
			
			
			Elements Series = doc.select("div.series>a");
			if (!Series.isEmpty()) {
				seriesstring =  Series.first().text().trim();
				String serieshref =  Series.first().attr("href");
			}
			
			
			
			Elements authorgroup = doc.select("div.contrib-group");
			if (authorgroup.isEmpty()) {
				Elements authorlist = authorgroup.first().getElementsByClass("name");
				for (Element authorobj:authorlist) {
					author += authorobj.text().trim()+";";
				}
				
			}
			author = StringHelper.cleanSemicolon(author);
			
			Elements cover = doc.select("img.cover");
			if (!cover.isEmpty()) {
				coverbase64 = cover.first().attr("src").trim();
			}
			
			
			String id = url.replace("/stable/", "");
			url = "https://www.jstor.org" + url;
			rawid = id;

			XXXXObject xObj = new XXXXObject();
			xObj.data.put("rawid", rawid);
			xObj.data.put("description", abstracts);
			xObj.data.put("publisher", contentPublisher);
			xObj.data.put("provider_subject", subject);
			xObj.data.put("title", title);
			xObj.data.put("page", page);
			xObj.data.put("identifier_doi", doi);
			xObj.data.put("date", date);
			xObj.data.put("date_created", date_created);
			xObj.data.put("country", country);
			xObj.data.put("language", language);
			xObj.data.put("url", url);
			xObj.data.put("author", author);
			xObj.data.put("seriesstring", seriesstring);
			xObj.data.put("batch", batch);
			xObj.data.put("down_date", "20181121");
			xObj.data.put("cover", coverbase64);
			xObj.data.put("eisbn", eisbn);
			xObj.data.put("edition", edition);
//			xObj.data.put("down_date", "20181120");
			xObj.data.put("down_date", down_date);

			context.getCounter("map", "count").increment(1);

			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(rawid), new BytesWritable(bytes));	

		}
	}

}
