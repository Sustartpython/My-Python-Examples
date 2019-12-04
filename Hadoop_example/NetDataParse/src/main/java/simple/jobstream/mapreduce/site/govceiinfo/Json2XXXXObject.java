package simple.jobstream.mapreduce.site.govceiinfo;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Stringifier;
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

//统计wos和ei的数据量
public class Json2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 5;
	private static int reduceNum = 100;

//	private static String batch = "";
	private static String inputHdfsPath = "";
	private static String outputHdfsPath = "";

//	public void pre(Job job) {
//		String jobName = "govceiinfo." + this.getClass().getSimpleName();
//		if (testRun) {
//			jobName = "test_" + jobName;
//		}
//		job.setJobName(jobName);
//
//		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
//		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
//	}
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
		job.setReducerClass(ProcessReducer.class);

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
//		static String doi = "";
		static String title = "";
//		static String keyword = "";
		static String description = "";
//		static String begin_page = "";
//		static String end_page = "";
//		static String raw_type = "";
//		static String recv_date = "";
//		static String accept_date = "";
		static String pub_date = "";
//		static String pub_date_alt = "";
		static String author = "";
//		static String organ = "";
//		static String journal_id = "";
//		static String journal_name = "";
		static String pub_year = "";
//		static String vol = "";
//		static String num = "";
//		static String publisher = "";
//		static String provider_url = "";
		static String description_source = "";
//		static String page = "";
		static String url = "";
		static String provider_subject = "";

		public void setup(Context context) throws IOException, InterruptedException {
			batch = context.getConfiguration().get("batch");
		}

		static String cleanSpace(String text) {
			text = text.replaceAll("[\\s\\p{Zs}]+", " ").trim();
			return text;
		}

		public static boolean parseHtml(Document doc) {
			
			//文章类型
			Element article_type1_element = doc.select("div.content").first();
			Element article_type2_element = doc.select("body[align=center]").first();
			if(article_type1_element!=null) {
				//标题,时间,来源
				Element h2_element = doc.select("div.xx_con_tile >h2").first();
				if(h2_element!=null) {
					String h2_text = h2_element.text().trim();
					title = h2_text;
					Element tl_element = h2_element.select("span.t_l").first();
					if(tl_element!=null) {
						String tl_text = tl_element.text();
						title = title.split("时间：")[0].trim();
						pub_date = tl_text.split("来源：")[0].replaceAll("时间：", "").trim();
						pub_year = pub_date.split("-")[0].trim();
						pub_date = pub_date.replaceAll("-", "").trim();
						String[] src_split = tl_text.split("来源：");
						if(src_split.length>1) {
							description_source = src_split[1].trim();
						}
					}
				}
				
				//摘要
				Element abs_element = doc.select("div.xx_con_1").first();
				if(abs_element!=null) {
					description = abs_element.text().replaceAll("查看全文", "").trim();
				}
				
				//provider_subject
				Element position_element = doc.select("div.nowPosition").first();
				if(position_element!=null) {
					String position_text = position_element.text().trim();
					String[] pStrings = position_text.split("- >");
					int len = pStrings.length;
					if(pStrings.length>1) {
						for(int i=1;i<len;i++) {
							provider_subject += pStrings[i].trim()+";";
						}
						provider_subject =provider_subject.replaceAll(";+$", "");
					}
				}
				
				
			}
			else if(article_type2_element!=null) {
				//标题,时间,来源
				Element title_element = doc.select("td.tab_a").first();
				if(title_element!=null) {
					String tba_text = title_element.text().trim();
					title = tba_text;
					Element tbb_element = title_element.select("span.tab_b").first();
					if(tbb_element!=null) {
						String tbb_text = tbb_element.text().trim();
						title = title.split("时间:")[0].trim();
						pub_date = tbb_text.split("来源：")[0].replaceAll("时间：", "").trim();
						pub_year = pub_date.split("-")[0].trim();
						pub_date = pub_date.replaceAll("-", "").trim();
						String[] src_split = tbb_text.split("来源：");
						if(src_split.length>1) {
							description_source = src_split[1].trim();
						}
					}
				}
				
				//摘要
				Element abs_element = doc.select("td.tab_d").first();
				if(abs_element!=null) {
					description = abs_element.text().trim();
				}
				
			}
//			
			
//			System.out.println(title);
//			System.out.println(pub_date);
//			System.out.println(pub_year);
//			System.out.println(description_source);
//			System.out.println(description);
//			System.out.println(provider_subject);
			return true;
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			{
				rawid = "";
				down_date = "";
//				doi = "";
				title = "";
//				keyword = "";
				description = "";
//				begin_page = "";
//				end_page = "";
//				raw_type = "";
//				recv_date = "";
//				accept_date = "";
				pub_date = "";
//				pub_date_alt = "";
//				author = "";
//				organ = "";
//				journal_id = "";
//				journal_name = "";
				pub_year = "";
//				vol = "";
//				num = "";
//				publisher = "";
//				provider_url = "";
				description_source = "";
				url="";
				provider_subject="";
			}

			Gson gson = new Gson();
			Type type = new TypeToken<Map<String, String>>() {
			}.getType();
			Map<String, String> mapJson = gson.fromJson(value.toString(), type);

			down_date = mapJson.get("down_date").trim();

			// rawid 文章唯一标识
			rawid = mapJson.get("rawid").trim();
			
			url = mapJson.get("url").trim();
			String[] url_split = url.split("=");
			
			String referCode = url_split[1].split("&")[0];
			String columnId = url_split[2];
			

			String html = mapJson.get("html").trim();
			Document doc = Jsoup.parse(html);

			// 解析html
			parseHtml(doc);

			XXXXObject xObj = new XXXXObject();

			xObj.data.put("rawid", rawid);
			xObj.data.put("down_date", down_date);
			xObj.data.put("batch", batch);
//			xObj.data.put("doi", doi);
			xObj.data.put("title", title);
//			xObj.data.put("keyword", keyword);
			xObj.data.put("description", description);
//			xObj.data.put("begin_page", begin_page);
//			xObj.data.put("end_page", end_page);
//			xObj.data.put("raw_type", raw_type);
//			xObj.data.put("recv_date", recv_date);
//			xObj.data.put("accept_date", accept_date);
			xObj.data.put("pub_date", pub_date);
//			xObj.data.put("pub_date_alt", pub_date_alt);
			xObj.data.put("author", author);
//			xObj.data.put("organ", organ);
//			xObj.data.put("journal_id", journal_id);
//			xObj.data.put("journal_name", journal_name);
			xObj.data.put("pub_year", pub_year);
//			xObj.data.put("vol", vol);
//			xObj.data.put("num", num);
//			xObj.data.put("publisher", publisher);
//			xObj.data.put("provider_url", provider_url);
//			
//			xObj.data.put("page", page);
			xObj.data.put("description_source", description_source);
			xObj.data.put("url", url);
			xObj.data.put("provider_subject", provider_subject);
			xObj.data.put("referCode", referCode);
			xObj.data.put("columnId", columnId);

			context.getCounter("map", "count").increment(1);

			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(rawid), new BytesWritable(bytes));

		}

	}

	// 继承Reducer接口，设置Reduce的输入类型为<Text,IntWritable>
	// 输出类型为<Text,IntWritable>
	public static class ProcessReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
		public void reduce(Text key, Iterable<BytesWritable> values, Context context)
				throws IOException, InterruptedException {
			/*
			 * Text out = new Text() for (BytesWritable val:values){
			 * VipcloudUtil.DeserializeObject(val.getBytes(), out); context.write(key,out);
			 * } //context.getCounter("reduce", "count").increment(1);
			 */
			BytesWritable bOut = new BytesWritable();
			for (BytesWritable item : values) {
				if (item.getLength() > bOut.getLength()) {
					bOut.set(item.getBytes(), 0, item.getLength());
				}
			}

			context.getCounter("reduce", "count").increment(1);

			bOut.setCapacity(bOut.getLength());

			context.write(key, bOut);

		}
	}
}