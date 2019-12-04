package simple.jobstream.mapreduce.site.rucjournal;

import java.io.IOException;
import java.util.Map;
import java.util.regex.*;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.util.StringHelper;

//输入应该为去重后的html
public class Last2A extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger
			.getLogger(Last2A.class);
	
	private static boolean testRun = false;
	private static int testReduceNum = 1;
	private static int reduceNum = 2;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";
	
	private static String postfixDb3 = "ezuezhe";

	
	public void pre(Job job) {
		String jobName = job.getConfiguration().get("jobName");
		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
		reduceNum = Integer.parseInt(job.getConfiguration().get("reduceNum"));
		job.setJobName(jobName);
	}

	public void post(Job job) {

	}

	public String getHdfsInput() {
		return inputHdfsPath;
	}

	public String getHdfsOutput() {
		return outputHdfsPath;
	}

	public void SetMRInfo(Job job) {
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));
//		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);

		job.setMapperClass(ProcessMapper.class);
//		job.setReducerClass(ProcessReducer.class);
		
//	    job.setInputFormatClass(TextInputFormat.class);
//	    job.setOutputFormatClass(SequenceFileOutputFormat.class);

		TextOutputFormat.setCompressOutput(job, false);

		if (testRun) {
			job.setNumReduceTasks(testReduceNum);
		} else {
			job.setNumReduceTasks(reduceNum);
		}
	}

	// ======================================处理逻辑=======================================
	public static class ProcessMapper extends
			Mapper<Text, BytesWritable, Text, BytesWritable> {
		
		Pattern patYearNum = Pattern.compile("(\\d{4})年([a-zA-Z0-9]+)期");
		
		public void setup(Context context) throws IOException,
				InterruptedException {
			
		}

		public void cleanup(Context context) throws IOException,
				InterruptedException {
			
		}

		
		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			
			String lngid = "";
			String rawid = "";
			String titletype = "0;1;256;257";
			String pna = "";
			String years = "";
			String title = "";
			String showorgan = "";
			String etil = "";
			String keyword = "";
			String keyword_alt = "";
			String abstracts = "";
			String east = "";
			String medias_qk = "";
			String language = "";
			String type = "";
			String author_1st = "";
			String Introduce = "";
			String srcID = "";
			String range = "";
			String srcproducer = "";
			String includeid = "";
//			String netfulltextaddr_all = "" ;
//			String netfulltextaddr_all_std = "" ;
			String provider_url = "";
			String aut = "";
			String aino = "";
			String translator = "";
			String still = "";
			String opc = "";
			String oad = "";
			String opy = "";
			String opn = "";
			String opg = "";
			String ori_src = "";
			String num = "";
			String sub_db_id = "";
			String product = "";
			String sub_db = "";
			String provider = "";
			String down_date = "";
			String batch = "";
			String til = "";
			String stil = "";
			String fund = "";
			
			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("lngid")) {
					lngid = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("stil")) {
					stil = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("til")) {
					til = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("aut")) {
					aut = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("showorgan")) {
					showorgan = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("etil")) {
					etil = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("keyword")) {
					keyword = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("keyword_alt")) {
					keyword_alt = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("media_qk")) {
					medias_qk = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("abstracts")) {
					abstracts = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("pna")) {
					pna = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("language")) {
					language = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("type")) {
					type = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("author_1st")) {
					author_1st = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("Introduce")) {
					Introduce = updateItem.getValue().trim();
//				}else if (updateItem.getKey().equals("netfulltextaddr_all")) {
//					netfulltextaddr_all = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("provider_url")) {
					provider_url = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("aut")) {
					aut = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("aino")) {
					aino = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("taut")) {
					translator = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("still")) {
					still = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("opc")) {
					opc = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("oad")) {
					oad = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("opy")) {
					opy = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("opn")) {
					opn = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("opg")) {
					opg = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("ori_src")) {
					ori_src = updateItem.getValue().trim();
//				}else if (updateItem.getKey().equals("netfulltextaddr_all_std")){
//					netfulltextaddr_all_std = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("sub_db_id")){
					sub_db_id = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("product")){
					product = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("sub_db")){
					sub_db = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("provider")){
					provider = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("down_date")){
					down_date = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("batch")){
					batch = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("east")){
					east = updateItem.getValue().trim();
				}else if(updateItem.getKey().equals("tno")){
					fund = updateItem.getValue().trim();
				}
			}
			
			title = til+stil;
		
			rawid = rawid.replace('\0', ' ').replace("'", "''").trim();
			
			srcID = "RUC";
			range = "RDFYBKZL";
			srcproducer = "RDFYBKZL";
			includeid = "[RDFYBKZL]" + rawid;
			fund = fund.replace('\0', ' ').replace("'", "''").trim();
			titletype = titletype.replace('\0', ' ').replace("'", "''").trim();
			pna = pna.replace('\0', ' ').replace("'", "''").trim();
			years = years.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			title = title.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			showorgan = showorgan.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			etil = etil.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			keyword = keyword.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			keyword_alt = keyword_alt.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			abstracts = abstracts.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			east = east.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			medias_qk = medias_qk.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			language = language.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			type = type.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			author_1st = author_1st.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			Introduce = Introduce.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			srcID = srcID.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			range = range.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			srcproducer = srcproducer.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			includeid = includeid.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			//netfulltextaddr_all = netfulltextaddr_all.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			provider_url = provider_url.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			//netfulltextaddr_all_std = netfulltextaddr_all_std.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			aut = aut.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			aino = aino.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			translator = translator.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			still = still.replace('\0', ' ').replace("'", "''").trim().replace("——", "").replace("\n", "");
			opc = opc.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			oad = oad.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			opy = opy.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			opn = opn.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			opg = opg.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			ori_src = ori_src.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			Matcher matYearNum = patYearNum.matcher(medias_qk);
			if (matYearNum.find()) {
				years = matYearNum.group(1);
				num = matYearNum.group(2);
			}else{
				context.getCounter("map", "years null").increment(1);
			}
			String jump_page="";
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
			String translator_intro = "";
			String translatorstring = "";
			String[] translatorArray = translator.split(" ");
			if (pageArray.length > 1) {
				translatorstring = translatorArray[0].trim();
				translator_intro = translator.replace(translatorArray[0]+" ", "").trim();
			}else {
				translatorstring = translator.trim();
			}
			
			aut = StringHelper.cleanSemicolon(aut);
			aut = aut.trim();
			// a 表结构
			XXXXObject xObj_a_table = new XXXXObject();
			xObj_a_table.data.put("lngid", lngid);
			xObj_a_table.data.put("rawid", rawid);
			xObj_a_table.data.put("sub_db_id", sub_db_id);
			xObj_a_table.data.put("product", product);
			xObj_a_table.data.put("sub_db", sub_db);
			xObj_a_table.data.put("provider", provider);
			xObj_a_table.data.put("down_date", down_date);
			xObj_a_table.data.put("batch", batch);
			xObj_a_table.data.put("source_type", "3");
			xObj_a_table.data.put("provider_url", provider_url);
			xObj_a_table.data.put("title", title);
			xObj_a_table.data.put("title_alt", etil);
			xObj_a_table.data.put("keyword", keyword);
			xObj_a_table.data.put("keyword_alt", keyword_alt);
			xObj_a_table.data.put("abstract", abstracts);
			xObj_a_table.data.put("abstract_alt", east);
			xObj_a_table.data.put("pageinfo", opg);
			xObj_a_table.data.put("begin_page", begin_page);
			xObj_a_table.data.put("end_page", end_page);
			xObj_a_table.data.put("jump_page", jump_page);
			xObj_a_table.data.put("pub_date", years+"0000");
			xObj_a_table.data.put("pub_year", years);
			xObj_a_table.data.put("author", aut);
			xObj_a_table.data.put("author_1st", author_1st);
			xObj_a_table.data.put("author_intro", Introduce);
			xObj_a_table.data.put("num", num);
			xObj_a_table.data.put("journal_name", pna);
			xObj_a_table.data.put("title_sub", still);
			//原文出处
			xObj_a_table.data.put("ori_src", ori_src);
			//译者
			xObj_a_table.data.put("translator", translatorstring);
			xObj_a_table.data.put("translator_intro", translator_intro);
			xObj_a_table.data.put("country", "CN");
			xObj_a_table.data.put("language", "ZH");
			xObj_a_table.data.put("fund", fund);
			
			
			context.getCounter("map", "count").increment(1);
			byte[] bytes = com.process.frame.util.VipcloudUtil.SerializeObject(xObj_a_table);
			context.write(new Text(rawid), new BytesWritable(bytes));	
			
		}
	}

}