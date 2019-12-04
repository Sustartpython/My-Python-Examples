package simple.jobstream.mapreduce.site.newrucjournal;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.print.DocFlavor.STRING;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.json.JSONArray;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import simple.jobstream.mapreduce.common.util.StringHelper;
import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class Json2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 10;
	private static int reduceNum = 10;

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
		job.getConfiguration().set("io.compression.codecs",
				"org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println(job.getConfiguration().get("io.compression.codecs"));

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(UniqXXXXObjectReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);

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

	public void cleanup(Context context) throws IOException, InterruptedException {

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

		Pattern patYearNum = Pattern.compile("(\\d{4})年([a-zA-Z0-9]+)期");

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			cnt += 1;
			if (cnt == 1) {
				System.out.println("text:" + value.toString());
			}
			
			String text = value.toString().trim();
			
			//json
			Gson gson = new Gson();
			Type type1 = new TypeToken<Map<String, Object>>() {}.getType();
				
			Map<String, Object> mapField = gson.fromJson(text, type1);
			
			String basexml = mapField.get("basexml").toString();
			String textxml = mapField.get("textxml").toString();
			String rawid = mapField.get("id").toString();
			String down_date = mapField.get("downdate").toString();

			HashMap<String, String> map = ParseXML(basexml,textxml);
			if (map != null) {

				XXXXObject xObj = new XXXXObject();
				
				String til = map.get("til");

				String stil = map.get("stil");
				String etil = map.get("etil");
				String dns = map.get("dns");
				String cls = map.get("cls");
				String aut = map.get("aut");
				String aino = map.get("aino");
				String taut = map.get("taut");
				String opc = map.get("opc");
				String oad = map.get("oad");
				String opy = map.get("opy");
				String opn = map.get("opn");
				String opg = map.get("opg");
				String ast = map.get("ast");
				String east = map.get("east");
				String pde = map.get("pde");
				String pna = map.get("pna");
				String py = map.get("py");
				String pno = map.get("pno");
				String fyno = map.get("fyno");
				String kew = map.get("kew");
				String tno = map.get("tno");
				String tcode = map.get("tcode");

				String lngid = "";
				String titletype = "";
				String keyword = "";
				String keyword_alt = "";
				String abstracts = "";
//				String showorgan = "";
				String medias_qk = "";
				String language = "ZH";
				String type = "3";
				String author_1st = "";
				String Introduce = "";
//				String srcID = "RUC";
//				String range = "RDFYBKZL";
//				String srcproducer = "RDFYBKZL";
//				String includeid = "[RDFYBKZL]" + rawid;
//				JSONObject netfulltextaddr_all;
//				JSONObject netfulltextaddr_all_std;
				String provider_url;
				String ori_src = "";
				String num = "";
				
				for (String item : kew.split("\\p{Zs}{2,}|#")) {
				
					item = item.trim();

					String pattern = "(\\p{IsHan})";
					// 创建 Pattern 对象
					Pattern r = Pattern.compile(pattern, Pattern.UNICODE_CHARACTER_CLASS);
					// 现在创建 matcher 对象
					Matcher m = r.matcher(item);
					if (m.find()) {
						keyword = keyword + item + ";";
					} else {
						keyword_alt = keyword_alt + item + ";";
					}

				}
				keyword = keyword.replaceAll("/", ";");
				keyword_alt = keyword_alt.replaceAll("/", ";");
				keyword = StringHelper.cleanSemicolon(keyword);
				keyword_alt = StringHelper.cleanSemicolon(keyword_alt);

				if (opg == "") {
					ori_src = "《" + opc + "》" + oad + opy + "年第" + opn + "期" + "\n";
				} else {
					ori_src = "《" + opc + "》" + oad + opy + "年第" + opn + "期" + " 第" + opg + "页" + "\n";
				}
				abstracts =  ast;

				aut = aut.replace('#', ';');
				author_1st = aut.split("#")[0];
				Introduce = aino + '\n' + taut;
				medias_qk = fyno;
				String tautstr = "";
				if (taut != "") {
					taut = taut.trim().replace("\n", "").replace('　', ' ').replace("（", "(").replace("选择", " ")
							.replace('1', ' ');
					tautstr = "";
					for (char a : taut.toCharArray()) {
						if ((a != ' ') & (a != '，') & (a != '：') & (a != '(')) {
							tautstr = tautstr + a;
						}
						if (a == ' ') {
							break;
						}
					}
					if (tautstr.length() > 12) {
						tautstr = "";
					}
					tautstr = ";" + tautstr.replace('/', ';');
					taut = tautstr;
					taut = StringHelper.cleanSemicolon(taut);
				}
				//作者由该数据组合
				//aut = aut + tautstr;
//				includeid = "[RDFYBKZL]" + rawid;
				provider_url = "http://www.rdfybk.com/qk/pdfview?id=" + rawid;
//				netfulltextaddr_all = createnetfulltextaddr_all(rawid);
//				netfulltextaddr_all_std = createnetfulltextaddr_all_std(rawid);
			

				Matcher matYearNum = patYearNum.matcher(medias_qk);
				if (matYearNum.find()) {
					py = matYearNum.group(1);
					num = matYearNum.group(2);
				} else {
					context.getCounter("map", "years null").increment(1);
				}
//				String netfulltextaddr_all_std_string = createnetfulltextaddr_all_std(rawid).toString();
//				String netfulltextaddr_all_string = createnetfulltextaddr_all(rawid).toString();
				rawid = rawid.replace('\0', ' ').replace("'", "''").trim();
				String sub_db_id = "00114";
				String product = "RDFYBK";
				String sub_db = "QK";
				String provider_zt = "rdfybkjournal";
				String provider = "RUC";

				lngid = VipIdEncode.getLngid(sub_db_id, rawid, false);

				String jump_page = "";
				String begin_page = "";
				String end_page = "";
				// 分割页面 page 获取里里面的详细信息
				opg = opg.replace("，", ",");
				String[] opgStrArray = opg.split(",");
				if (opgStrArray.length == 2) {
					jump_page = opgStrArray[1].trim();
				}
				String[] pageArray = opgStrArray[0].split("-");
				if (pageArray.length == 2) {
					begin_page = pageArray[0].trim();
					end_page = pageArray[1].trim();
				}


				xObj.data.put("lngid", lngid);

				xObj.data.put("rawid", rawid);
				xObj.data.put("product", product);
				// a表不要该字段
				xObj.data.put("titletype", titletype);
				xObj.data.put("num", num);
				xObj.data.put("sub_db_id", sub_db_id);
				xObj.data.put("sub_db", sub_db);
				xObj.data.put("provider_zt", provider_zt);
				xObj.data.put("provider", provider);
				
				xObj.data.put("dns", dns);
				xObj.data.put("cls", cls);
				xObj.data.put("pde", pde);
				xObj.data.put("pno", pno);
				xObj.data.put("fyno", fyno);
				
				xObj.data.put("jump_page", jump_page);
				xObj.data.put("begin_page", begin_page);
				xObj.data.put("end_page", end_page);
				//期刊名
				xObj.data.put("pna", pna);
				//期刊年
				xObj.data.put("py", py);
				xObj.data.put("til", til);
				xObj.data.put("stil", stil);
				xObj.data.put("etil", etil);
				xObj.data.put("tno", tno);
				xObj.data.put("tcode", tcode);
				
				// 关键词
				xObj.data.put("kew", kew);
				xObj.data.put("keyword", keyword);
				xObj.data.put("keyword_alt", keyword_alt);
				//摘要
				xObj.data.put("ast", ast);
				xObj.data.put("east", east);
				xObj.data.put("abstracts", abstracts);
				//作者
				xObj.data.put("aut", aut);
				//译者
				xObj.data.put("taut", taut);
//				xObj.data.put("showorgan", showorgan);

				xObj.data.put("media_qk", medias_qk);
				xObj.data.put("language", language);
				xObj.data.put("type", type);

				xObj.data.put("author_1st", author_1st);
				xObj.data.put("Introduce", Introduce);
				xObj.data.put("provider_url", provider_url);
				
//				xObj.data.put("srcID", srcID);
//				xObj.data.put("range", range);
//				xObj.data.put("srcproducer", srcproducer);
//				xObj.data.put("includeid", includeid);
			
//				xObj.data.put("netfulltextaddr_all", netfulltextaddr_all.toString());
//				xObj.data.put("netfulltextaddr_all_string", netfulltextaddr_all_string);
//				xObj.data.put("netfulltextaddr_all_std_string", netfulltextaddr_all_std_string);
				// xObj.data.put("netfulltextaddr_all_std", netfulltextaddr_all_std.toString());
				//作者简介
				xObj.data.put("aino", aino);
				xObj.data.put("still", stil);
				xObj.data.put("opc", opc);
				xObj.data.put("oad", oad);
				xObj.data.put("opy", opy);
				xObj.data.put("opn", opn);
				xObj.data.put("opg", opg);
				xObj.data.put("ori_src", ori_src);

				xObj.data.put("batch", batch);
				xObj.data.put("down_date", down_date);

				byte[] bytes = com.process.frame.util.VipcloudUtil.SerializeObject(xObj);
				context.getCounter("map", "count").increment(1);
				context.write(new Text(rawid), new BytesWritable(bytes));
			} else {
				context.getCounter("map", "null").increment(1);
				return;
			}
		}
	}


	public static HashMap<String, String> ParseXML(String xml,String astxml) {
		HashMap<String, String> map = new HashMap<String, String>();
		try {

			try {
				DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
				DocumentBuilder builder = dbFactory.newDocumentBuilder();

				Document doc = builder.parse(new InputSource(new StringReader(xml)));
				NodeList nList = doc.getElementsByTagName("R1");
				Element node = (Element) nList.item(0);
				// 标题
				String til;
				//副标题
				String stil;
				//外文标题
				String etil;
				//不清楚 
				String dns;
				//一个年份 但页面无显示
				String cls;
				//作者
				String aut;
				//作者简介
				String aino;
				//
				String taut;
				//原文出处
				String opc;
				//原文地址
				String oad;
				//原文年
				String opy;
				//原文期
				String opn;
				//原文页码信息
				String opg;
				//内容提要
				String ast;
				//英文内容提要
				String east;
				//(例)N2 
				String pde;
				//期刊名称
				String pna;
				//期刊年
				String py;
				//期刊期
				String pno;
				//期刊年期（2019年01期）
				String fyno;
				//关键字
				String kew;
				//标题注释 目前知道里面是基金信息
				String tno;
				//(例)05
				String tcode;

				try {
					til = node.getElementsByTagName("til").item(0).getFirstChild().getTextContent();
				} catch (Exception e) {
					til = "";
				}

				try {
					stil = node.getElementsByTagName("stil").item(0).getFirstChild().getTextContent();
				} catch (Exception e) {
					stil = "";
				}

				try {
					etil = node.getElementsByTagName("etil").item(0).getFirstChild().getTextContent();
				} catch (Exception e) {
					etil = "";
				}

				try {
					dns = node.getElementsByTagName("dns").item(0).getFirstChild().getTextContent();
				} catch (Exception e) {
					dns = "";
				}

				try {
					cls = node.getElementsByTagName("cls").item(0).getFirstChild().getTextContent();
				} catch (Exception e) {
					cls = "";
				}

				try {
					aut = node.getElementsByTagName("aut").item(0).getFirstChild().getTextContent();
				} catch (Exception e) {
					aut = "";
				}

				try {
					aino = node.getElementsByTagName("aino").item(0).getFirstChild().getTextContent();
				} catch (Exception e) {
					aino = "";
				}

				try {
					taut = node.getElementsByTagName("taut").item(0).getFirstChild().getTextContent();
				} catch (Exception e) {
					taut = "";
				}

				try {
					opc = node.getElementsByTagName("opc").item(0).getFirstChild().getTextContent();
				} catch (Exception e) {
					opc = "";
				}

				try {
					oad = node.getElementsByTagName("oad").item(0).getFirstChild().getTextContent();
				} catch (Exception e) {
					oad = "";
				}

				try {
					opy = node.getElementsByTagName("opy").item(0).getFirstChild().getTextContent();
				} catch (Exception e) {
					opy = "";
				}

				try {
					opn = node.getElementsByTagName("opn").item(0).getFirstChild().getTextContent();
				} catch (Exception e) {
					opn = "";
				}

				try {
					opg = node.getElementsByTagName("opg").item(0).getFirstChild().getTextContent();
				} catch (Exception e) {
					opg = "";
				}

				try {
					ast = node.getElementsByTagName("ast").item(0).getFirstChild().getTextContent();
				} catch (Exception e) {
					ast = "";
				}
				try {
					east = node.getElementsByTagName("east").item(0).getFirstChild().getTextContent();
				} catch (Exception e) {
					east = "";
				}

				try {
					pde = node.getElementsByTagName("pde").item(0).getFirstChild().getTextContent();
				} catch (Exception e) {
					pde = "";
				}

				try {
					pna = node.getElementsByTagName("pna").item(0).getFirstChild().getTextContent();
				} catch (Exception e) {
					pna = "";
				}

				try {
					py = node.getElementsByTagName("py").item(0).getFirstChild().getTextContent();
				} catch (Exception e) {
					py = "";
				}

				try {
					pno = node.getElementsByTagName("pno").item(0).getFirstChild().getTextContent();
				} catch (Exception e) {
					pno = "";
				}

				try {
					fyno = node.getElementsByTagName("fyno").item(0).getFirstChild().getTextContent();
				} catch (Exception e) {
					fyno = "";
				}

				try {
					kew = node.getElementsByTagName("kew").item(0).getFirstChild().getTextContent();
				} catch (Exception e) {
					kew = "";
				}

				try {
					tno = node.getElementsByTagName("tno").item(0).getFirstChild().getTextContent();
				} catch (Exception e) {
					tno = "";
				}

				try {
					tcode = node.getElementsByTagName("tcode").item(0).getFirstChild().getTextContent();
				} catch (Exception e) {
					tcode = "";
				}

				if (ast == "") {
					try {
						dbFactory = DocumentBuilderFactory.newInstance();
						builder = dbFactory.newDocumentBuilder();

						doc = builder.parse(new InputSource(new StringReader(astxml)));
						nList = doc.getElementsByTagName("R1");

						node = (Element) nList.item(0);
						try {
							ast = node.getElementsByTagName("ctt").item(0).getFirstChild().getTextContent();
							ast = ast.replace("</p></p>", "").trim().replace("　　", "");

						} catch (Exception e) {
							ast = "";
						}
					} catch (Exception e) {
						ast = "";
					}

				}

				map.put("til", til);

				map.put("stil", stil);
				map.put("etil", etil);
				map.put("dns", dns);
				map.put("cls", cls);
				map.put("aut", aut);
				map.put("aino", aino);
				map.put("taut", taut);
				map.put("opc", opc);
				map.put("oad", oad);
				map.put("opy", opy);
				map.put("opn", opn);
				map.put("opg", opg);
				map.put("ast", ast);
				map.put("east", east);
				map.put("pde", pde);
				map.put("pna", pna);
				map.put("py", py);
				map.put("pno", pno);
				map.put("fyno", fyno);
				map.put("kew", kew);
				map.put("tno", tno);
				map.put("tcode", tcode);

				return map;
			} catch (Exception e) {
				map = null;
				return map;

			}
		} catch (Exception e) {
			map = null;
			return map;
		}
	}
}
