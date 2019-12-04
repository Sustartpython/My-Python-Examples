package simple.jobstream.mapreduce.site.cssci;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;
import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer4Ref;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;
import simple.jobstream.mapreduce.site.cssci.CssciDetail;
import simple.jobstream.mapreduce.site.cssci.CssciDetail.Author;
import simple.jobstream.mapreduce.site.cssci.CssciDetail.Catation;
import simple.jobstream.mapreduce.site.cssci.CssciDetail.Contents;
import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.util.StringHelper;

//统计wos和ei的数据量
public class Json2RefXXXXObject extends InHdfsOutHdfsJobInfo {
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
		job.setReducerClass(UniqXXXXObjectReducer4Ref.class);

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
	// 继承Mapper接口,设置map的输入类型为<Object,Text>
	// 输出类型为<Text,IntWritable>
	public static class ProcessMapper extends Mapper<LongWritable, Text, Text, BytesWritable> {
		public String batch = "";
		private static Map<String, String> maptype = new HashMap<String, String>();

		private static void initMapType() {
			maptype.put("1", "J");// 期刊论文
			maptype.put("11", "Z");// 电子文献
			maptype.put("10", "Z");// 法规
			maptype.put("9", "S");// 标准
			maptype.put("8", "R");// 报告
			maptype.put("7", "G");// 汇编
			maptype.put("6", "Z");// 信件
			maptype.put("5", "D");// 学位论文
			maptype.put("4", "C");// 会议文献
			maptype.put("3", "N");// 报纸
			maptype.put("2", "M");// 图书
			maptype.put("99", "Z");// 其他
		}

		public void setup(Context context) throws IOException, InterruptedException {
			batch = context.getConfiguration().get("batch");
			initMapType();
		}

		public static String[] getPage(String text) {
			text = text.replaceAll("-+|\\+-|－", "-").replaceAll("[;|,|\\-|\\.|\\]|`]$", "").replaceAll("\\s?-\\s", "-");
			text = text.replaceAll("，|、|;|；",",").replaceAll("^,", "");
			String[] result = new String[3];
			String begin_page = "";
			String end_page = "";
			String jump_page = "";
			Matcher pagetype1 = Pattern.compile("(\\w+-\\w+)-(\\w+-\\w+),(\\w+-\\w+)").matcher(text);
			Matcher pagetype2 = Pattern.compile("(\\D+-\\w+)[-|,](\\D+-\\w+)").matcher(text);
			Matcher pagetype3 = Pattern.compile("P?(\\w+)-(\\w+)[,|;|\\s|\\.|/](\\w+)$").matcher(text);
			Matcher pagetype4 = Pattern.compile("^P?(\\D)-(\\d+)$").matcher(text);
			Matcher pagetype5 = Pattern.compile("^P?(\\w+)-(\\w+)$").matcher(text);
			if (pagetype1.find()) {
				begin_page = pagetype1.group(1);
				end_page = pagetype1.group(2);
				jump_page = pagetype1.group(3);
			} else if (pagetype2.find()) {
				begin_page = pagetype2.group(1);
				end_page = pagetype2.group(2);
			} else if (pagetype3.find()) {
				begin_page = pagetype3.group(1);
				end_page = pagetype3.group(2);
				jump_page = pagetype3.group(3);
			} else if (pagetype4.find()) {
				begin_page = text;
				end_page = text;
			} else if (pagetype5.find()) {
				begin_page = pagetype5.group(1);
				end_page = pagetype5.group(2);
			} else {
				if (!text.contains(",")) {
					begin_page = text;
					end_page = text;
				}
			}
			result[0] = begin_page;
			result[1] = end_page;
			result[2] = jump_page;
			return result;
		}

		public static String getReferTextSite(Catation catation) {
			String show_catation = "";
			if (catation.ywlx.equals("1"))// 期刊论文
			{
				String tmp = catation.ywcc.replace(catation.ywnd + "，", "");
				tmp = tmp.replace(catation.ywnd + ".", "");
				tmp = tmp.replace(catation.ywnd + "（", "（");
				tmp = tmp.replace(catation.ywnd + ",", "");
				show_catation = catation.ywzz + "." + catation.ywpm + "." + catation.ywqk;

				if (catation.ywnd.length() > 2) {
					show_catation += "." + catation.ywnd;
				}
				show_catation += "," + tmp;
			} else if (catation.ywlx.equals("3"))// 报纸
			{
				show_catation = catation.ywzz + "." + catation.ywqk;

				if (catation.ywcc.length() > 0) {
					show_catation += "." + catation.ywcc;
				} else {
					show_catation += "." + catation.ywnd;
				}
			} else if (catation.ywlx.equals("11"))// 电子文献,网站
			{
				show_catation = catation.ywzz + "." + catation.ywpm;
				show_catation += "." + catation.ywqk;
				if (catation.ywnd.length() > 2) {
					show_catation += "." + catation.ywnd;
				}
			} else if (catation.ywlx.equals("99"))// 其它
			{
				show_catation = catation.ywpm;
			} else if (catation.ywlx.equals("7"))// 汇编
			{
				if (catation.ywzz.trim().length() > 0) {
					show_catation += catation.ywzz;
				}
				if (catation.ywpm.trim().length() > 0) {
					show_catation += "." + catation.ywpm;
				}

				if (catation.ywcc.length() > 0) {
					show_catation += "." + catation.ywcc;
				} else {
					if (catation.ywqk.trim().length() > 0) {
						show_catation += ":" + catation.ywqk;
					}
					if (catation.ywcbd.trim().length() > 0) {
						show_catation += "." + catation.ywcbd;
					}
					if (catation.ywcbs.trim().length() > 0) {
						show_catation += ":" + catation.ywcbs;
					}
					if (catation.ywnd.trim().length() > 2) {
						show_catation += "," + catation.ywnd;
					}
					if (catation.ywym.length() > 0) {
						show_catation += ":" + catation.ywym;
					}
				}
			} else {
				// 作者.书名.出版地：出版社，出版年.起止页码
				if (catation.ywzz.trim().length() > 0) {
					show_catation += catation.ywzz;
				}
				if (catation.ywpm.trim().length() > 0) {
					show_catation += "." + catation.ywpm;
				}
				if (catation.ywcbd.trim().length() > 0) {
					show_catation += "." + catation.ywcbd;
				}
				if (catation.ywcbs.trim().length() > 0) {
					show_catation += ":" + catation.ywcbs;
				}
				if (catation.ywnd.trim().length() > 2) {
					show_catation += "," + catation.ywnd;
				}
				if (catation.ywym.length() > 0) {
					show_catation += ":" + catation.ywym;
				}
			}
			return show_catation;

		}

		public static String[] getVolNum(String text) {
			String[] result = new String[2];
			String vol = "";
			String num = "";
			Matcher voltype1 = Pattern.compile("(\\d+)\\((\\w+)\\)").matcher(text);
			Matcher voltype2 = Pattern.compile("\\((\\w+)\\)").matcher(text);
			if (voltype1.find()) {
				vol = voltype1.group(1);
				num = voltype1.group(2);
			} else if (voltype2.find()) {
				num = voltype2.group(1);
			}
			result[0] = vol;
			result[1] = num;
			return result;
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String author = "";
			String author_1st = "";
			String title = "";
			String title_alt = "";
			String pub_year = "";
			String vol = "";
			String num = "";
			String publisher = "";
			String source_name = "";
			String raw_type = "";
			String page_info = "";
			String begin_page = "";
			String end_page = "";
			String jump_page = "";
			String refer_text_raw = value.toString();
			String refer_text_site = "";
			String refer_text = "";
			String strtype = "";

			String lngid = "";
			String cited_id = "";
			String product = "CSSCI";
			String sub_db = "QK";
			String provider = "NJU";
			String sub_db_id = "00028";
//			String down_date = "20190425";

			Gson gson = new Gson();
//			CssciDetail pJson = null;
			Type jsonmaptype = new TypeToken<Map<String, String>>() {
			}.getType();
			Map<String, String> mapJson = gson.fromJson(value.toString(), jsonmaptype);

			String down_date = mapJson.get("down_date").trim();
			String jsonString = mapJson.get("html").trim();
			Type type = new TypeToken<Map<String, JsonElement>>() {}.getType();
			Map<String, JsonElement> mapField = null;
			try {
				mapField = gson.fromJson(jsonString, type);
			} catch (Exception e) {
				context.getCounter("map", "jsonerror").increment(1);
				return;
			}
			if (mapField.get("catation").isJsonNull()) {
				return;
			}
			int cnt = 1;
			for (JsonElement refJsonElement : mapField.get("catation").getAsJsonArray()) {
				refer_text_raw = refJsonElement.toString();
				Catation catation = gson.fromJson(refJsonElement, Catation.class);
				title = catation.ywpm;
				if (!catation.ywnd.equals("0")) {
					pub_year = catation.ywnd;
				}				
				String[] vol_num = getVolNum(catation.ywcc);
				vol = vol_num[0];
				num = vol_num[1];
				author = catation.ywzz;
				author_1st = author;
				if (!catation.ywcbd.equals("")) {
					publisher = catation.ywcbd;
					if (!catation.ywcbs.equals("")) {
						publisher = catation.ywcbd + ":" + catation.ywcbs;
					}
				}
				else if (!catation.ywcbs.equals("")) {
					publisher = catation.ywcbs;
				}
				
				source_name = catation.ywqk;
				raw_type = catation.ywlx;
				if (maptype.containsKey(catation.ywlx)) {
					strtype = maptype.get(catation.ywlx);
				} else {
					strtype = "";
				}
				page_info = catation.ywym;
//				String[] page = getPage(page_info);
//				begin_page = page[0];
//				end_page = page[1];
//				jump_page = page[2];
				refer_text_site = getReferTextSite(catation);
				if (refer_text_site.equals("")) {
					context.getCounter("map", "refer_text_site is null").increment(1);
					continue;					
				}

				refer_text = refer_text_site;

				cited_id = VipIdEncode.getLngid(sub_db_id, catation.sno, false) + "@" + catation.sno;
				String citeno = String.format("%04d", cnt);
//				if (String.valueOf(cnt).length() == 1) {
//					citeno = "000" + String.valueOf(cnt);
//				} else if (String.valueOf(cnt).length() == 2) {
//					citeno = "00" + String.valueOf(cnt);
//				} else if (String.valueOf(cnt).length() == 3) {
//					citeno = "0" + String.valueOf(cnt);
//				} else if (String.valueOf(cnt).length() == 4) {
//					citeno = String.valueOf(cnt);
//				} else {
//					context.getCounter("map", "citeno>9999").increment(1);
//					continue;
//				}
				lngid = VipIdEncode.getLngid(sub_db_id, catation.sno, false) + citeno;
				cnt += 1;

				XXXXObject xObj = new XXXXObject();

				xObj.data.put("author", author);
				xObj.data.put("author_1st", author_1st);
				xObj.data.put("title", title);
				xObj.data.put("title_alt", title_alt);
				xObj.data.put("pub_year", pub_year);
				xObj.data.put("vol", vol);
				xObj.data.put("num", num);
				xObj.data.put("publisher", publisher);
				xObj.data.put("source_name", source_name);
				xObj.data.put("raw_type", raw_type);
				xObj.data.put("page_info", page_info);
				xObj.data.put("begin_page", begin_page);
				xObj.data.put("end_page", end_page);
				xObj.data.put("jump_page", jump_page);
				xObj.data.put("refer_text_raw", refer_text_raw);
				xObj.data.put("refer_text_site", refer_text_site);
				xObj.data.put("refer_text", refer_text);
				xObj.data.put("strtype", strtype);

				xObj.data.put("lngid", lngid);
				xObj.data.put("cited_id", cited_id);
				xObj.data.put("product", product);
				xObj.data.put("sub_db", sub_db);
				xObj.data.put("provider", provider);
				xObj.data.put("sub_db_id", sub_db_id);
				xObj.data.put("batch", batch);
				xObj.data.put("down_date", down_date);
				

				context.getCounter("map", "count").increment(1);

				byte[] bytes = VipcloudUtil.SerializeObject(xObj);
				context.write(new Text(catation.sno), new BytesWritable(bytes));
			}
			// try {
			// pJson = gson.fromJson(value.toString(), CssciDetail.class);
			// } catch (Exception e) {
			// context.getCounter("map", "jsonerror").increment(1);
			// return;
			// }
			//
			// if (pJson.catation == null) {
			// return;
			// }
			// int cnt = 1;
			//
			// for (Catation catation : pJson.catation) {
			// title = catation.ywpm;
			// pub_year = catation.ywnd;
			// String[] vol_num = getVolNum(catation.ywcc);
			// vol = vol_num[0];
			// num = vol_num[1];
			// author = catation.ywzz;
			// author_1st = author;
			// publisher = catation.ywcbd + ":" + catation.ywcbs;
			// source_name = catation.ywqk;
			// raw_type = catation.ywlx;
			// if (maptype.containsKey(catation.ywlx)) {
			// strtype = maptype.get(catation.ywlx);
			// }
			// else {
			// strtype = "";
			// }
			// page_info = catation.ywym;
			// String[] page = getPage(page_info);
			// begin_page = page[0];
			// end_page = page[1];
			// jump_page = page[2];
			// refer_text_raw = line;
			// refer_text_site = getReferTextSite(String.valueOf(cnt),
			// catation);
			//
			// refer_text = refer_text_site;
			//
			// cited_id = VipIdEncode.getLngid(sub_db_id,catation.sno,false) +
			// "@" + catation.sno;
			// String citeno = "";
			// if (String.valueOf(cnt).length()==1) {
			// citeno = "000" + String.valueOf(cnt);
			// }
			// else if (String.valueOf(cnt).length()==2) {
			// citeno = "00" + String.valueOf(cnt);
			// }
			// else if (String.valueOf(cnt).length()==3) {
			// citeno = "0" + String.valueOf(cnt);
			// }
			// else if (String.valueOf(cnt).length()==4) {
			// citeno = String.valueOf(cnt);
			// }
			// else {
			// context.getCounter("map", "citeno>9999").increment(1);
			// continue;
			// }
			// lngid = cited_id + citeno;
			// cnt += 1;
			//
			// XXXXObject xObj = new XXXXObject();
			//
			// xObj.data.put("author",author);
			// xObj.data.put("author_1st",author_1st);
			// xObj.data.put("title",title);
			// xObj.data.put("title_alt",title_alt);
			// xObj.data.put("pub_year",pub_year);
			// xObj.data.put("vol",vol);
			// xObj.data.put("num",num);
			// xObj.data.put("publisher",publisher);
			// xObj.data.put("source_name",source_name);
			// xObj.data.put("raw_type",raw_type);
			// xObj.data.put("page_info",page_info);
			// xObj.data.put("begin_page",begin_page);
			// xObj.data.put("end_page",end_page);
			// xObj.data.put("jump_page",jump_page);
			// xObj.data.put("refer_text_raw",refer_text_raw);
			// xObj.data.put("refer_text_site",refer_text_site);
			// xObj.data.put("refer_text",refer_text);
			// xObj.data.put("strtype",strtype);
			//
			//
			// xObj.data.put("lngid",lngid);
			// xObj.data.put("cited_id",cited_id);
			// xObj.data.put("product",product);
			// xObj.data.put("sub_db",sub_db);
			// xObj.data.put("provider",provider);
			// xObj.data.put("sub_db_id",sub_db_id);
			// xObj.data.put("batch", batch);
			//
			// context.getCounter("map", "count").increment(1);
			//
			// byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			// context.write(new Text(cited_id), new BytesWritable(bytes));
			// }

		}

	}
}