package simple.jobstream.mapreduce.site.ebsco_cmedm;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.*;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.almworks.sqlite4java.SQLiteConnection;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.BXXXXObject;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.JobConfUtil;
import com.process.frame.util.SimpleTextInputFormat;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.vip.SqliteReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;



//输入应该为去重后的html
public class StdXXXXobject_zt extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger
			.getLogger(StdXXXXobject_zt.class);
	
	private static boolean testRun = true;
	private static int testReduceNum = 3;
	private static int reduceNum = 3;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";
	private static String postfixDb3 = "ebscojournal";
	

	
	public void pre(Job job) {
		String jobName = "Ebsco_cmedm_" + this.getClass().getSimpleName();

		job.setJobName(jobName);
		
		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
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
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(SqliteReducer.class);

		TextOutputFormat.setCompressOutput(job, false);

		 job.setOutputValueClass(BytesWritable.class);
		 JobConfUtil.setTaskPerReduceMemory(job, 6144);
		if (testRun) {
			job.setNumReduceTasks(testReduceNum);
		} else {
			job.setNumReduceTasks(reduceNum);
		}
	}

	// ======================================处理逻辑=======================================
	public static class ProcessMapper extends
			Mapper<Text, BytesWritable, Text, NullWritable> {
		
		
		private static Map<String, String> monthMap = new HashMap<String, String>();
		private static Map<String, String> langShort = new HashMap<String,String>();
		private static Map<String, String> nationMap = new HashMap<String, String>();

		private static void initMonthmonthMap() {
			monthMap.put("january", "01");
			monthMap.put("jan", "01");
			monthMap.put("jan.", "01");
			monthMap.put("february", "02");
			monthMap.put("feb", "02");
			monthMap.put("feb.", "02");
			monthMap.put("march", "03");
			monthMap.put("mar", "03");
			monthMap.put("mar.", "03");
			monthMap.put("april", "04");
			monthMap.put("apr", "04");
			monthMap.put("apr.", "04");
			monthMap.put("may", "05");
			monthMap.put("may.", "05");
			monthMap.put("june", "06");
			monthMap.put("jun", "06");
			monthMap.put("jun.", "06");
			monthMap.put("july", "07");
			monthMap.put("jul", "07");
			monthMap.put("jul.", "07");
			monthMap.put("august", "08");
			monthMap.put("aug", "08");
			monthMap.put("aug.", "08");
			monthMap.put("september", "09");
			monthMap.put("sept", "09");
			monthMap.put("sept.", "09");
			monthMap.put("sep", "09");
			monthMap.put("sep.", "09");
			monthMap.put("october", "10");
			monthMap.put("oct", "10");
			monthMap.put("oct.", "10");
			monthMap.put("november", "11");
			monthMap.put("nov", "11");
			monthMap.put("nov.", "11");
			monthMap.put("december", "12");
			monthMap.put("dezember", "12");
			monthMap.put("dec", "12");
			monthMap.put("dec.", "12");
			monthMap.put("oktober", "10");
			monthMap.put("juni", "06");
		}
		
		public String getMapValueByKey(String mykey) {
			String value = "00";
			for (Map.Entry entry : monthMap.entrySet()) {

				String key = entry.getKey().toString();
				if (mykey.toLowerCase().startsWith(key)) {
					value = entry.getValue().toString();
					break;

				}

			}
			return value;

		}
		
		private static void InitLangShortMap(){
			langShort.put("aa", "AA");
			langShort.put("ab", "AB");
			langShort.put("ae", "AE");
			langShort.put("af", "AF");
			langShort.put("ak", "AK");
			langShort.put("am", "AM");
			langShort.put("an", "AN");
			langShort.put("ar", "AR");
			langShort.put("as", "AS");
			langShort.put("av", "AV");
			langShort.put("ay", "AY");
			langShort.put("az", "AZ");
			langShort.put("ba", "BA");
			langShort.put("be", "BE");
			langShort.put("bg", "BG");
			langShort.put("bh", "BH");
			langShort.put("bi", "BI");
			langShort.put("bm", "BM");
			langShort.put("bn", "BN");
			langShort.put("bo", "BO");
			langShort.put("br", "BR");
			langShort.put("bs", "BS");
			langShort.put("ca", "CA");
			langShort.put("ce", "CE");
			langShort.put("ch", "CH");
			langShort.put("co", "CO");
			langShort.put("cr", "CR");
			langShort.put("cs", "CS");
			langShort.put("cu", "CU");
			langShort.put("cv", "CV");
			langShort.put("cy", "CY");
			langShort.put("da", "DA");
			langShort.put("de", "DE");
			langShort.put("dv", "DV");
			langShort.put("dz", "DZ");
			langShort.put("ee", "EE");
			langShort.put("el", "EL");
			langShort.put("en", "EN");
			langShort.put("eo", "EO");
			langShort.put("es", "ES");
			langShort.put("et", "ET");
			langShort.put("eu", "EU");
			langShort.put("fa", "FA");
			langShort.put("ff", "FF");
			langShort.put("fi", "FI");
			langShort.put("fj", "FJ");
			langShort.put("fo", "FO");
			langShort.put("fr", "FR");
			langShort.put("fy", "FY");
			langShort.put("ga", "GA");
			langShort.put("gd", "GD");
			langShort.put("gl", "GL");
			langShort.put("gn", "GN");
			langShort.put("gu", "GU");
			langShort.put("gv", "GV");
			langShort.put("ha", "HA");
			langShort.put("he", "HE");
			langShort.put("hi", "HI");
			langShort.put("ho", "HO");
			langShort.put("hr", "HR");
			langShort.put("ht", "HT");
			langShort.put("hu", "HU");
			langShort.put("hy", "HY");
			langShort.put("hz", "HZ");
			langShort.put("ia", "IA");
			langShort.put("id", "ID");
			langShort.put("ie", "IE");
			langShort.put("ig", "IG");
			langShort.put("ii", "II");
			langShort.put("ik", "IK");
			langShort.put("io", "IO");
			langShort.put("is", "IS");
			langShort.put("it", "IT");
			langShort.put("iu", "IU");
			langShort.put("ja", "JA");
			langShort.put("jv", "JV");
			langShort.put("ka", "KA");
			langShort.put("kg", "KG");
			langShort.put("ki", "KI");
			langShort.put("kj", "KJ");
			langShort.put("kk", "KK");
			langShort.put("kl", "KL");
			langShort.put("km", "KM");
			langShort.put("kn", "KN");
			langShort.put("ko", "KO");
			langShort.put("kr", "KR");
			langShort.put("ks", "KS");
			langShort.put("ku", "KU");
			langShort.put("kv", "KV");
			langShort.put("kw", "KW");
			langShort.put("ky", "KY");
			langShort.put("la", "LA");
			langShort.put("lb", "LB");
			langShort.put("lg", "LG");
			langShort.put("li", "LI");
			langShort.put("ln", "LN");
			langShort.put("lo", "LO");
			langShort.put("lt", "LT");
			langShort.put("lu", "LU");
			langShort.put("lv", "LV");
			langShort.put("mg", "MG");
			langShort.put("mh", "MH");
			langShort.put("mi", "MI");
			langShort.put("mk", "MK");
			langShort.put("ml", "ML");
			langShort.put("mn", "MN");
			langShort.put("mo", "MO");
			langShort.put("mr", "MR");
			langShort.put("ms", "MS");
			langShort.put("mt", "MT");
			langShort.put("my", "MY");
			langShort.put("na", "NA");
			langShort.put("nb", "NB");
			langShort.put("nd", "ND");
			langShort.put("ne", "NE");
			langShort.put("ng", "NG");
			langShort.put("nl", "NL");
			langShort.put("nn", "NN");
			langShort.put("no", "NO");
			langShort.put("nr", "NR");
			langShort.put("nv", "NV");
			langShort.put("ny", "NY");
			langShort.put("oc", "OC");
			langShort.put("oj", "OJ");
			langShort.put("om", "OM");
			langShort.put("or", "OR");
			langShort.put("os", "OS");
			langShort.put("pa", "PA");
			langShort.put("pi", "PI");
			langShort.put("pl", "PL");
			langShort.put("ps", "PS");
			langShort.put("pt", "PT");
			langShort.put("qu", "QU");
			langShort.put("rm", "RM");
			langShort.put("rn", "RN");
			langShort.put("ro", "RO");
			langShort.put("ru", "RU");
			langShort.put("rw", "RW");
			langShort.put("sa", "SA");
			langShort.put("sc", "SC");
			langShort.put("sd", "SD");
			langShort.put("se", "SE");
			langShort.put("sg", "SG");
			langShort.put("sh", "SH");
			langShort.put("si", "SI");
			langShort.put("sk", "SK");
			langShort.put("sl", "SL");
			langShort.put("sm", "SM");
			langShort.put("sn", "SN");
			langShort.put("so", "SO");
			langShort.put("sq", "SQ");
			langShort.put("sr", "SR");
			langShort.put("ss", "SS");
			langShort.put("st", "ST");
			langShort.put("su", "SU");
			langShort.put("sv", "SV");
			langShort.put("sw", "SW");
			langShort.put("ta", "TA");
			langShort.put("te", "TE");
			langShort.put("tg", "TG");
			langShort.put("th", "TH");
			langShort.put("ti", "TI");
			langShort.put("tk", "TK");
			langShort.put("tl", "TL");
			langShort.put("tn", "TN");
			langShort.put("to", "TO");
			langShort.put("tr", "TR");
			langShort.put("ts", "TS");
			langShort.put("tt", "TT");
			langShort.put("tw", "TW");
			langShort.put("ty", "TY");
			langShort.put("ug", "UG");
			langShort.put("uk", "UK");
			langShort.put("ur", "UR");
			langShort.put("uz", "UZ");
			langShort.put("ve", "VE");
			langShort.put("vi", "VI");
			langShort.put("vo", "VO");
			langShort.put("wa", "WA");
			langShort.put("wo", "WO");
			langShort.put("xh", "XH");
			langShort.put("yi", "YI");
			langShort.put("yo", "YO");
			langShort.put("za", "ZA");
			langShort.put("zh", "ZH");
			langShort.put("zu", "ZU");
			langShort.put("aar", "AA");
			langShort.put("afar", "AA");
			langShort.put("abk", "AB");
			langShort.put("abkhazian", "AB");
			langShort.put("ave", "AE");
			langShort.put("avestan", "AE");
			langShort.put("afr", "AF");
			langShort.put("afrikaans", "AF");
			langShort.put("akan", "AK");
			langShort.put("aka + 2", "AK");
			langShort.put("aka", "AK");
			langShort.put("amh", "AM");
			langShort.put("amharic", "AM");
			langShort.put("arg", "AN");
			langShort.put("aragonese", "AN");
			langShort.put("arabic", "AR");
			langShort.put("ara + 30", "AR");
			langShort.put("ara", "AR");
			langShort.put("assamese", "AS");
			langShort.put("asm", "AS");
			langShort.put("ava", "AV");
			langShort.put("avaric", "AV");
			langShort.put("aymara", "AY");
			langShort.put("aym + 2", "AY");
			langShort.put("aym", "AY");
			langShort.put("aze", "AZ");
			langShort.put("azerbaijani", "AZ");
			langShort.put("aze + 2", "AZ");
			langShort.put("bashkir", "BA");
			langShort.put("bak", "BA");
			langShort.put("belarusian", "BE");
			langShort.put("bel", "BE");
			langShort.put("bul", "BG");
			langShort.put("bulgarian", "BG");
			langShort.put("bihari", "BH");
			langShort.put("bih", "BH");
			langShort.put("none", "BH");
			langShort.put("bis", "BI");
			langShort.put("bislama", "BI");
			langShort.put("bambara", "BM");
			langShort.put("bam", "BM");
			langShort.put("bengali", "BN");
			langShort.put("ben", "BN");
			langShort.put("bod", "BO");
			langShort.put("tib", "BO");
			langShort.put("tibetan", "BO");
			langShort.put("breton", "BR");
			langShort.put("bre", "BR");
			langShort.put("bosnian", "BS");
			langShort.put("bos", "BS");
			langShort.put("catalan", "CA");
			langShort.put("cat", "CA");
			langShort.put("che", "CE");
			langShort.put("chechen", "CE");
			langShort.put("chamorro", "CH");
			langShort.put("cha", "CH");
			langShort.put("corsican", "CO");
			langShort.put("cos", "CO");
			langShort.put("cre", "CR");
			langShort.put("cree", "CR");
			langShort.put("cre + 6", "CR");
			langShort.put("ces", "CS");
			langShort.put("czech", "CS");
			langShort.put("cze", "CS");
			langShort.put("chu", "CU");
			langShort.put("church slavic", "CU");
			langShort.put("chv", "CV");
			langShort.put("chuvash", "CV");
			langShort.put("cym", "CY");
			langShort.put("wel", "CY");
			langShort.put("welsh", "CY");
			langShort.put("danish", "DA");
			langShort.put("dan", "DA");
			langShort.put("ger", "DE");
			langShort.put("deu", "DE");
			langShort.put("german", "DE");
			langShort.put("divehi", "DV");
			langShort.put("div", "DV");
			langShort.put("dzo", "DZ");
			langShort.put("dzongkha", "DZ");
			langShort.put("ewe", "EE");
			langShort.put("ell", "EL");
			langShort.put("greek", "EL");
			langShort.put("gre", "EL");
			langShort.put("english", "EN");
			langShort.put("eng", "EN");
			langShort.put("epo", "EO");
			langShort.put("esperanto", "EO");
			langShort.put("castilian", "ES");
			langShort.put("spanish", "ES");
			langShort.put("spa", "ES");
			langShort.put("est", "ET");
			langShort.put("estonian", "ET");
			langShort.put("basque", "EU");
			langShort.put("baq", "EU");
			langShort.put("eus", "EU");
			langShort.put("fas + 2", "FA");
			langShort.put("fas + 1", "FA");
			langShort.put("fas", "FA");
			langShort.put("per", "FA");
			langShort.put("persian", "FA");
			langShort.put("ful", "FF");
			langShort.put("ful + 9", "FF");
			langShort.put("fulah", "FF");
			langShort.put("fin", "FI");
			langShort.put("finnish", "FI");
			langShort.put("fijian", "FJ");
			langShort.put("fij", "FJ");
			langShort.put("faroese", "FO");
			langShort.put("fao", "FO");
			langShort.put("french", "FR");
			langShort.put("fre", "FR");
			langShort.put("fra", "FR");
			langShort.put("fry", "FY");
			langShort.put("western frisian", "FY");
			langShort.put("fry + 3", "FY");
			langShort.put("gle", "GA");
			langShort.put("irish", "GA");
			langShort.put("scottish gaelic", "GD");
			langShort.put("gla", "GD");
			langShort.put("glg", "GL");
			langShort.put("galician", "GL");
			langShort.put("guaraní", "GN");
			langShort.put("grn + 5", "GN");
			langShort.put("grn", "GN");
			langShort.put("guj", "GU");
			langShort.put("gujarati", "GU");
			langShort.put("glv", "GV");
			langShort.put("manx", "GV");
			langShort.put("hausa", "HA");
			langShort.put("hau", "HA");
			langShort.put("hebrew", "HE");
			langShort.put("heb", "HE");
			langShort.put("hindi", "HI");
			langShort.put("hin", "HI");
			langShort.put("hmo", "HO");
			langShort.put("hiri motu", "HO");
			langShort.put("scr", "HR");
			langShort.put("croatian", "HR");
			langShort.put("hrv", "HR");
			langShort.put("hat", "HT");
			langShort.put("haitian creole", "HT");
			langShort.put("hungarian", "HU");
			langShort.put("hun", "HU");
			langShort.put("arm", "HY");
			langShort.put("armenian", "HY");
			langShort.put("hye", "HY");
			langShort.put("herero", "HZ");
			langShort.put("her", "HZ");
			langShort.put("ina", "IA");
			langShort.put("interlingua", "IA");
			langShort.put("international auxiliary language association", "IA");
			langShort.put("ind", "ID");
			langShort.put("indonesian", "ID");
			langShort.put("interlingue", "IE");
			langShort.put("ile", "IE");
			langShort.put("igbo", "IG");
			langShort.put("ibo", "IG");
			langShort.put("iii", "II");
			langShort.put("sichuan yi", "II");
			langShort.put("ipk", "IK");
			langShort.put("ipk + 2", "IK");
			langShort.put("inupiaq", "IK");
			langShort.put("ido", "IO");
			langShort.put("icelandic", "IS");
			langShort.put("ice", "IS");
			langShort.put("isl", "IS");
			langShort.put("italian", "IT");
			langShort.put("ita", "IT");
			langShort.put("iku + 2", "IU");
			langShort.put("inuktitut", "IU");
			langShort.put("iku", "IU");
			langShort.put("jpn", "JA");
			langShort.put("japanese", "JA");
			langShort.put("jav", "JV");
			langShort.put("javanese", "JV");
			langShort.put("geo", "KA");
			langShort.put("georgian", "KA");
			langShort.put("kat", "KA");
			langShort.put("kongo", "KG");
			langShort.put("kon + 3", "KG");
			langShort.put("kon", "KG");
			langShort.put("kik", "KI");
			langShort.put("kikuyu", "KI");
			langShort.put("kua", "KJ");
			langShort.put("kwanyama", "KJ");
			langShort.put("kazakh", "KK");
			langShort.put("kaz", "KK");
			langShort.put("kal", "KL");
			langShort.put("kalaallisut", "KL");
			langShort.put("khm", "KM");
			langShort.put("khmer", "KM");
			langShort.put("kan", "KN");
			langShort.put("kannada", "KN");
			langShort.put("korean", "KO");
			langShort.put("kor", "KO");
			langShort.put("kau + 3", "KR");
			langShort.put("kau", "KR");
			langShort.put("kanuri", "KR");
			langShort.put("kashmiri", "KS");
			langShort.put("kas", "KS");
			langShort.put("kurdish", "KU");
			langShort.put("kur + 3", "KU");
			langShort.put("kur", "KU");
			langShort.put("komi", "KV");
			langShort.put("kom", "KV");
			langShort.put("kom + 2", "KV");
			langShort.put("cornish", "KW");
			langShort.put("cor", "KW");
			langShort.put("kirghiz", "KY");
			langShort.put("kir", "KY");
			langShort.put("latin", "LA");
			langShort.put("lat", "LA");
			langShort.put("luxembourgish", "LB");
			langShort.put("ltz", "LB");
			langShort.put("ganda", "LG");
			langShort.put("lug", "LG");
			langShort.put("lim", "LI");
			langShort.put("limburgish", "LI");
			langShort.put("lingala", "LN");
			langShort.put("lin", "LN");
			langShort.put("lao", "LO");
			langShort.put("lithuanian", "LT");
			langShort.put("lit", "LT");
			langShort.put("lub", "LU");
			langShort.put("luba-katanga", "LU");
			langShort.put("lav", "LV");
			langShort.put("latvian", "LV");
			langShort.put("mlg + 10", "MG");
			langShort.put("mlg", "MG");
			langShort.put("malagasy", "MG");
			langShort.put("marshallese", "MH");
			langShort.put("mah", "MH");
			langShort.put("mri", "MI");
			langShort.put("mao", "MI");
			langShort.put("māori", "MI");
			langShort.put("mac", "MK");
			langShort.put("macedonian", "MK");
			langShort.put("mkd", "MK");
			langShort.put("mal", "ML");
			langShort.put("malayalam", "ML");
			langShort.put("mongolian", "MN");
			langShort.put("mon + 2", "MN");
			langShort.put("mon", "MN");
			langShort.put("mol", "MO");
			langShort.put("moldavian", "MO");
			langShort.put("mar", "MR");
			langShort.put("marathi", "MR");
			langShort.put("msa + 12", "MS");
			langShort.put("msa", "MS");
			langShort.put("msa + 13", "MS");
			langShort.put("may", "MS");
			langShort.put("malay", "MS");
			langShort.put("maltese", "MT");
			langShort.put("mlt", "MT");
			langShort.put("burmese", "MY");
			langShort.put("mya", "MY");
			langShort.put("bur", "MY");
			langShort.put("nauru", "NA");
			langShort.put("nau", "NA");
			langShort.put("nob", "NB");
			langShort.put("norwegian bokmål", "NB");
			langShort.put("nde", "ND");
			langShort.put("north ndebele", "ND");
			langShort.put("nepali", "NE");
			langShort.put("nep", "NE");
			langShort.put("ndo", "NG");
			langShort.put("ndonga", "NG");
			langShort.put("nld", "NL");
			langShort.put("dut", "NL");
			langShort.put("dutch", "NL");
			langShort.put("norwegian nynorsk", "NN");
			langShort.put("nno", "NN");
			langShort.put("nor + 2", "NO");
			langShort.put("nor", "NO");
			langShort.put("norwegian", "NO");
			langShort.put("south ndebele", "NR");
			langShort.put("nbl", "NR");
			langShort.put("nav", "NV");
			langShort.put("navajo", "NV");
			langShort.put("chichewa", "NY");
			langShort.put("nya", "NY");
			langShort.put("oci", "OC");
			langShort.put("occitan", "OC");
			langShort.put("oji", "OJ");
			langShort.put("oji + 7", "OJ");
			langShort.put("ojibwa", "OJ");
			langShort.put("oromo", "OM");
			langShort.put("orm + 4", "OM");
			langShort.put("orm", "OM");
			langShort.put("oriya", "OR");
			langShort.put("ori", "OR");
			langShort.put("oss", "OS");
			langShort.put("ossetian", "OS");
			langShort.put("pan", "PA");
			langShort.put("panjabi", "PA");
			langShort.put("pli", "PI");
			langShort.put("pāli", "PI");
			langShort.put("pol", "PL");
			langShort.put("polish", "PL");
			langShort.put("pashto", "PS");
			langShort.put("pus + 3", "PS");
			langShort.put("pus", "PS");
			langShort.put("portuguese", "PT");
			langShort.put("por", "PT");
			langShort.put("que", "QU");
			langShort.put("que + 44", "QU");
			langShort.put("quechua", "QU");
			langShort.put("raeto-romance", "RM");
			langShort.put("roh", "RM");
			langShort.put("kirundi", "RN");
			langShort.put("run", "RN");
			langShort.put("rum", "RO");
			langShort.put("romanian", "RO");
			langShort.put("ron", "RO");
			langShort.put("russian", "RU");
			langShort.put("rus", "RU");
			langShort.put("kin", "RW");
			langShort.put("kinyarwanda", "RW");
			langShort.put("san", "SA");
			langShort.put("sanskrit", "SA");
			langShort.put("srd", "SC");
			langShort.put("srd + 4", "SC");
			langShort.put("sardinian", "SC");
			langShort.put("sindhi", "SD");
			langShort.put("snd", "SD");
			langShort.put("northern sami", "SE");
			langShort.put("sme", "SE");
			langShort.put("sag", "SG");
			langShort.put("sango", "SG");
			langShort.put("hbs + 3", "SH");
			langShort.put("hbs ", "SH");
			langShort.put("serbo-croatian", "SH");
			langShort.put("sinhalese", "SI");
			langShort.put("sin", "SI");
			langShort.put("slk", "SK");
			langShort.put("slo", "SK");
			langShort.put("slovak", "SK");
			langShort.put("slv", "SL");
			langShort.put("slovenian", "SL");
			langShort.put("samoan", "SM");
			langShort.put("smo", "SM");
			langShort.put("shona", "SN");
			langShort.put("sna", "SN");
			langShort.put("somali", "SO");
			langShort.put("som", "SO");
			langShort.put("albanian", "SQ");
			langShort.put("sqi + 4", "SQ");
			langShort.put("sqi + 3", "SQ");
			langShort.put("sqi", "SQ");
			langShort.put("alb", "SQ");
			langShort.put("scc", "SR");
			langShort.put("srp", "SR");
			langShort.put("serbian", "SR");
			langShort.put("swati", "SS");
			langShort.put("ssw", "SS");
			langShort.put("sotho", "ST");
			langShort.put("sot", "ST");
			langShort.put("sundanese", "SU");
			langShort.put("sun", "SU");
			langShort.put("swedish", "SV");
			langShort.put("swe", "SV");
			langShort.put("swahili", "SW");
			langShort.put("swa + 2", "SW");
			langShort.put("swa", "SW");
			langShort.put("tamil", "TA");
			langShort.put("tam", "TA");
			langShort.put("tel", "TE");
			langShort.put("telugu", "TE");
			langShort.put("tgk", "TG");
			langShort.put("tajik", "TG");
			langShort.put("thai", "TH");
			langShort.put("tha", "TH");
			langShort.put("tir", "TI");
			langShort.put("tigrinya", "TI");
			langShort.put("tuk", "TK");
			langShort.put("turkmen", "TK");
			langShort.put("tagalog", "TL");
			langShort.put("tgl", "TL");
			langShort.put("tswana", "TN");
			langShort.put("tsn", "TN");
			langShort.put("ton", "TO");
			langShort.put("tonga", "TO");
			langShort.put("turkish", "TR");
			langShort.put("tur", "TR");
			langShort.put("tso", "TS");
			langShort.put("tsonga", "TS");
			langShort.put("tatar", "TT");
			langShort.put("tat", "TT");
			langShort.put("twi", "TW");
			langShort.put("tahitian", "TY");
			langShort.put("tah", "TY");
			langShort.put("uyghur", "UG");
			langShort.put("uig", "UG");
			langShort.put("ukrainian", "UK");
			langShort.put("ukr", "UK");
			langShort.put("urd", "UR");
			langShort.put("urdu", "UR");
			langShort.put("uzb", "UZ");
			langShort.put("uzb + 2", "UZ");
			langShort.put("uzbek", "UZ");
			langShort.put("venda", "VE");
			langShort.put("ven", "VE");
			langShort.put("vietnamese", "VI");
			langShort.put("vie", "VI");
			langShort.put("vol", "VO");
			langShort.put("volapük", "VO");
			langShort.put("wln", "WA");
			langShort.put("walloon", "WA");
			langShort.put("wol", "WO");
			langShort.put("wolof", "WO");
			langShort.put("xho", "XH");
			langShort.put("xhosa", "XH");
			langShort.put("yid", "YI");
			langShort.put("yiddish", "YI");
			langShort.put("yid + 2", "YI");
			langShort.put("yoruba", "YO");
			langShort.put("yor", "YO");
			langShort.put("zhuang", "ZA");
			langShort.put("zha", "ZA");
			langShort.put("zha + 2", "ZA");
			langShort.put("zho + 13", "ZH");
			langShort.put("zho + 12", "ZH");
			langShort.put("zho", "ZH");
			langShort.put("chi", "ZH");
			langShort.put("chinese", "ZH");
			langShort.put("zul", "ZU");
			langShort.put("zulu", "ZU");

		}
		private static void InitNationMap(){
			nationMap.put("ad", "AD");
			nationMap.put("ae", "AE");
			nationMap.put("af", "AF");
			nationMap.put("ag", "AG");
			nationMap.put("ai", "AI");
			nationMap.put("al", "AL");
			nationMap.put("am", "AM");
			nationMap.put("ao", "AO");
			nationMap.put("aq", "AQ");
			nationMap.put("ar", "AR");
			nationMap.put("as", "AS");
			nationMap.put("at", "AT");
			nationMap.put("au", "AU");
			nationMap.put("aw", "AW");
			nationMap.put("ax", "AX");
			nationMap.put("az", "AZ");
			nationMap.put("ba", "BA");
			nationMap.put("bb", "BB");
			nationMap.put("bd", "BD");
			nationMap.put("be", "BE");
			nationMap.put("bf", "BF");
			nationMap.put("bg", "BG");
			nationMap.put("bh", "BH");
			nationMap.put("bi", "BI");
			nationMap.put("bj", "BJ");
			nationMap.put("bl", "BL");
			nationMap.put("bm", "BM");
			nationMap.put("bn", "BN");
			nationMap.put("bo", "BO");
			nationMap.put("bq", "BQ");
			nationMap.put("br", "BR");
			nationMap.put("bs", "BS");
			nationMap.put("bt", "BT");
			nationMap.put("bv", "BV");
			nationMap.put("bw", "BW");
			nationMap.put("by", "BY");
			nationMap.put("bz", "BZ");
			nationMap.put("ca", "CA");
			nationMap.put("cc", "CC");
			nationMap.put("cd", "CD");
			nationMap.put("cf", "CF");
			nationMap.put("cg", "CG");
			nationMap.put("ch", "CH");
			nationMap.put("ci", "CI");
			nationMap.put("ck", "CK");
			nationMap.put("cl", "CL");
			nationMap.put("cm", "CM");
			nationMap.put("cn", "CN");
			nationMap.put("co", "CO");
			nationMap.put("cr", "CR");
			nationMap.put("cu", "CU");
			nationMap.put("cv", "CV");
			nationMap.put("cw", "CW");
			nationMap.put("cx", "CX");
			nationMap.put("cy", "CY");
			nationMap.put("cz", "CZ");
			nationMap.put("de", "DE");
			nationMap.put("dj", "DJ");
			nationMap.put("dk", "DK");
			nationMap.put("dm", "DM");
			nationMap.put("do", "DO");
			nationMap.put("dz", "DZ");
			nationMap.put("ec", "EC");
			nationMap.put("ee", "EE");
			nationMap.put("eg", "EG");
			nationMap.put("eh", "EH");
			nationMap.put("er", "ER");
			nationMap.put("es", "ES");
			nationMap.put("et", "ET");
			nationMap.put("fi", "FI");
			nationMap.put("fj", "FJ");
			nationMap.put("fk", "FK");
			nationMap.put("fm", "FM");
			nationMap.put("fo", "FO");
			nationMap.put("fr", "FR");
			nationMap.put("ga", "GA");
			nationMap.put("gb", "GB");
			nationMap.put("gd", "GD");
			nationMap.put("ge", "GE");
			nationMap.put("gf", "GF");
			nationMap.put("gg", "GG");
			nationMap.put("gh", "GH");
			nationMap.put("gi", "GI");
			nationMap.put("gl", "GL");
			nationMap.put("gm", "GM");
			nationMap.put("gn", "GN");
			nationMap.put("gp", "GP");
			nationMap.put("gq", "GQ");
			nationMap.put("gr", "GR");
			nationMap.put("gs", "GS");
			nationMap.put("gt", "GT");
			nationMap.put("gu", "GU");
			nationMap.put("gw", "GW");
			nationMap.put("gy", "GY");
			nationMap.put("hk", "HK");
			nationMap.put("hm", "HM");
			nationMap.put("hn", "HN");
			nationMap.put("hr", "HR");
			nationMap.put("ht", "HT");
			nationMap.put("hu", "HU");
			nationMap.put("id", "ID");
			nationMap.put("ie", "IE");
			nationMap.put("il", "IL");
			nationMap.put("im", "IM");
			nationMap.put("in", "IN");
			nationMap.put("io", "IO");
			nationMap.put("iq", "IQ");
			nationMap.put("ir", "IR");
			nationMap.put("is", "IS");
			nationMap.put("it", "IT");
			nationMap.put("je", "JE");
			nationMap.put("jm", "JM");
			nationMap.put("jo", "JO");
			nationMap.put("jp", "JP");
			nationMap.put("ke", "KE");
			nationMap.put("kg", "KG");
			nationMap.put("kh", "KH");
			nationMap.put("ki", "KI");
			nationMap.put("km", "KM");
			nationMap.put("kn", "KN");
			nationMap.put("kp", "KP");
			nationMap.put("kr", "KR");
			nationMap.put("kw", "KW");
			nationMap.put("ky", "KY");
			nationMap.put("kz", "KZ");
			nationMap.put("la", "LA");
			nationMap.put("lb", "LB");
			nationMap.put("lc", "LC");
			nationMap.put("li", "LI");
			nationMap.put("lk", "LK");
			nationMap.put("lr", "LR");
			nationMap.put("ls", "LS");
			nationMap.put("lt", "LT");
			nationMap.put("lu", "LU");
			nationMap.put("lv", "LV");
			nationMap.put("ly", "LY");
			nationMap.put("ma", "MA");
			nationMap.put("mc", "MC");
			nationMap.put("md", "MD");
			nationMap.put("me", "ME");
			nationMap.put("mf", "MF");
			nationMap.put("mg", "MG");
			nationMap.put("mh", "MH");
			nationMap.put("mk", "MK");
			nationMap.put("ml", "ML");
			nationMap.put("mm", "MM");
			nationMap.put("mn", "MN");
			nationMap.put("mo", "MO");
			nationMap.put("mp", "MP");
			nationMap.put("mq", "MQ");
			nationMap.put("mr", "MR");
			nationMap.put("ms", "MS");
			nationMap.put("mt", "MT");
			nationMap.put("mu", "MU");
			nationMap.put("mv", "MV");
			nationMap.put("mw", "MW");
			nationMap.put("mx", "MX");
			nationMap.put("my", "MY");
			nationMap.put("mz", "MZ");
			nationMap.put("na", "NA");
			nationMap.put("nc", "NC");
			nationMap.put("ne", "NE");
			nationMap.put("nf", "NF");
			nationMap.put("ng", "NG");
			nationMap.put("ni", "NI");
			nationMap.put("nl", "NL");
			nationMap.put("no", "NO");
			nationMap.put("np", "NP");
			nationMap.put("nr", "NR");
			nationMap.put("nu", "NU");
			nationMap.put("nz", "NZ");
			nationMap.put("om", "OM");
			nationMap.put("pa", "PA");
			nationMap.put("pe", "PE");
			nationMap.put("pf", "PF");
			nationMap.put("pg", "PG");
			nationMap.put("ph", "PH");
			nationMap.put("pk", "PK");
			nationMap.put("pl", "PL");
			nationMap.put("pm", "PM");
			nationMap.put("pn", "PN");
			nationMap.put("pr", "PR");
			nationMap.put("ps", "PS");
			nationMap.put("pt", "PT");
			nationMap.put("pw", "PW");
			nationMap.put("py", "PY");
			nationMap.put("qa", "QA");
			nationMap.put("re", "RE");
			nationMap.put("ro", "RO");
			nationMap.put("rs", "RS");
			nationMap.put("ru", "RU");
			nationMap.put("rw", "RW");
			nationMap.put("sa", "SA");
			nationMap.put("sb", "SB");
			nationMap.put("sc", "SC");
			nationMap.put("sd", "SD");
			nationMap.put("se", "SE");
			nationMap.put("sg", "SG");
			nationMap.put("sh", "SH");
			nationMap.put("si", "SI");
			nationMap.put("sj", "SJ");
			nationMap.put("sk", "SK");
			nationMap.put("sl", "SL");
			nationMap.put("sm", "SM");
			nationMap.put("sn", "SN");
			nationMap.put("so", "SO");
			nationMap.put("sr", "SR");
			nationMap.put("ss", "SS");
			nationMap.put("st", "ST");
			nationMap.put("sv", "SV");
			nationMap.put("sx", "SX");
			nationMap.put("sy", "SY");
			nationMap.put("sz", "SZ");
			nationMap.put("tc", "TC");
			nationMap.put("td", "TD");
			nationMap.put("tf", "TF");
			nationMap.put("tg", "TG");
			nationMap.put("th", "TH");
			nationMap.put("tj", "TJ");
			nationMap.put("tk", "TK");
			nationMap.put("tl", "TL");
			nationMap.put("tm", "TM");
			nationMap.put("tn", "TN");
			nationMap.put("to", "TO");
			nationMap.put("tr", "TR");
			nationMap.put("tt", "TT");
			nationMap.put("tv", "TV");
			nationMap.put("tw", "TW");
			nationMap.put("tz", "TZ");
			nationMap.put("ua", "UA");
			nationMap.put("ug", "UG");
			nationMap.put("um", "UM");
			nationMap.put("us", "US");
			nationMap.put("uy", "UY");
			nationMap.put("uz", "UZ");
			nationMap.put("va", "VA");
			nationMap.put("vc", "VC");
			nationMap.put("ve", "VE");
			nationMap.put("vg", "VG");
			nationMap.put("vi", "VI");
			nationMap.put("vn", "VN");
			nationMap.put("vu", "VU");
			nationMap.put("wf", "WF");
			nationMap.put("ws", "WS");
			nationMap.put("ye", "YE");
			nationMap.put("yt", "YT");
			nationMap.put("za", "ZA");
			nationMap.put("zm", "ZM");
			nationMap.put("zw", "ZW");
			nationMap.put("and", "AD");
			nationMap.put("andorra", "AD");
			nationMap.put("are", "AE");
			nationMap.put("united arab emirates", "AE");
			nationMap.put("afghanistan", "AF");
			nationMap.put("afg", "AF");
			nationMap.put("atg", "AG");
			nationMap.put("antigua and barbuda", "AG");
			nationMap.put("aia", "AI");
			nationMap.put("anguilla", "AI");
			nationMap.put("albania", "AL");
			nationMap.put("alb", "AL");
			nationMap.put("arm", "AM");
			nationMap.put("armenia (republic)", "AM");
			nationMap.put("armenia", "AM");
			nationMap.put("angola", "AO");
			nationMap.put("ago", "AO");
			nationMap.put("antarctica", "AQ");
			nationMap.put("ata", "AQ");
			nationMap.put("arg", "AR");
			nationMap.put("argentina", "AR");
			nationMap.put("asm", "AS");
			nationMap.put("american samoa", "AS");
			nationMap.put("austria", "AT");
			nationMap.put("aut", "AT");
			nationMap.put("aus", "AU");
			nationMap.put("australia", "AU");
			nationMap.put("abw", "AW");
			nationMap.put("aruba", "AW");
			nationMap.put("ala", "AX");
			nationMap.put("åland islands", "AX");
			nationMap.put("azerbaijan", "AZ");
			nationMap.put("aze", "AZ");
			nationMap.put("bih", "BA");
			nationMap.put("bosnia and herzegovina", "BA");
			nationMap.put("brb", "BB");
			nationMap.put("barbados", "BB");
			nationMap.put("bangladesh", "BD");
			nationMap.put("bgd", "BD");
			nationMap.put("belgium", "BE");
			nationMap.put("bel", "BE");
			nationMap.put("burkina faso", "BF");
			nationMap.put("bfa", "BF");
			nationMap.put("bgr", "BG");
			nationMap.put("bulgaria", "BG");
			nationMap.put("bhr", "BH");
			nationMap.put("bahrain", "BH");
			nationMap.put("burundi", "BI");
			nationMap.put("bdi", "BI");
			nationMap.put("ben", "BJ");
			nationMap.put("benin", "BJ");
			nationMap.put("saint barthélemy", "BL");
			nationMap.put("blm", "BL");
			nationMap.put("bermuda", "BM");
			nationMap.put("bmu", "BM");
			nationMap.put("brn", "BN");
			nationMap.put("brunei darussalam", "BN");
			nationMap.put("bol", "BO");
			nationMap.put("bolivia (plurinational state of)", "BO");
			nationMap.put("bolivia", "BO");
			nationMap.put("bonaire, sint eustatius and saba", "BQ");
			nationMap.put("bes", "BQ");
			nationMap.put("bra", "BR");
			nationMap.put("brazil", "BR");
			nationMap.put("bhs", "BS");
			nationMap.put("bahamas", "BS");
			nationMap.put("bhutan", "BT");
			nationMap.put("btn", "BT");
			nationMap.put("bouvet island", "BV");
			nationMap.put("bvt", "BV");
			nationMap.put("bwa", "BW");
			nationMap.put("botswana", "BW");
			nationMap.put("blr", "BY");
			nationMap.put("belarus", "BY");
			nationMap.put("belize", "BZ");
			nationMap.put("blz", "BZ");
			nationMap.put("canada", "CA");
			nationMap.put("can", "CA");
			nationMap.put("cocos (keeling) islands", "CC");
			nationMap.put("cck", "CC");
			nationMap.put("cod", "CD");
			nationMap.put("congo (democratic republic)", "CD");
			nationMap.put("congo (democratic republic of the)", "CD");
			nationMap.put("central african republic", "CF");
			nationMap.put("caf", "CF");
			nationMap.put("cog", "CG");
			nationMap.put("congo (brazzaville)", "CG");
			nationMap.put("congo", "CG");
			nationMap.put("switzerland", "CH");
			nationMap.put("che", "CH");
			nationMap.put("cote d'ivoire", "CI");
			nationMap.put("côte d'ivoire", "CI");
			nationMap.put("civ", "CI");
			nationMap.put("cook islands", "CK");
			nationMap.put("cok", "CK");
			nationMap.put("chl", "CL");
			nationMap.put("chile", "CL");
			nationMap.put("cmr", "CM");
			nationMap.put("cameroon", "CM");
			nationMap.put("chn", "CN");
			nationMap.put("china", "CN");
			nationMap.put("china (republic : 1949- )", "CN");
			nationMap.put("col", "CO");
			nationMap.put("colombia", "CO");
			nationMap.put("cri", "CR");
			nationMap.put("costa rica", "CR");
			nationMap.put("cub", "CU");
			nationMap.put("cuba", "CU");
			nationMap.put("cabo verde", "CV");
			nationMap.put("cpv", "CV");
			nationMap.put("curaçao", "CW");
			nationMap.put("cuw", "CW");
			nationMap.put("christmas island", "CX");
			nationMap.put("cxr", "CX");
			nationMap.put("cyp", "CY");
			nationMap.put("cyprus", "CY");
			nationMap.put("czechia", "CZ");
			nationMap.put("czech republic", "CZ");
			nationMap.put("cze", "CZ");
			nationMap.put("germany", "DE");
			nationMap.put("deu", "DE");
			nationMap.put("djibouti", "DJ");
			nationMap.put("dji", "DJ");
			nationMap.put("dnk", "DK");
			nationMap.put("denmark", "DK");
			nationMap.put("dominica", "DM");
			nationMap.put("dma", "DM");
			nationMap.put("dominican republic", "DO");
			nationMap.put("dom", "DO");
			nationMap.put("algeria", "DZ");
			nationMap.put("dza", "DZ");
			nationMap.put("ecuador", "EC");
			nationMap.put("ecu", "EC");
			nationMap.put("est", "EE");
			nationMap.put("estonia", "EE");
			nationMap.put("egypt", "EG");
			nationMap.put("egy", "EG");
			nationMap.put("esh", "EH");
			nationMap.put("western sahara", "EH");
			nationMap.put("eritrea", "ER");
			nationMap.put("eri", "ER");
			nationMap.put("esp", "ES");
			nationMap.put("spain", "ES");
			nationMap.put("eth", "ET");
			nationMap.put("ethiopia", "ET");
			nationMap.put("finland", "FI");
			nationMap.put("fin", "FI");
			nationMap.put("fji", "FJ");
			nationMap.put("fiji", "FJ");
			nationMap.put("falkland islands (malvinas)", "FK");
			nationMap.put("flk", "FK");
			nationMap.put("fsm", "FM");
			nationMap.put("micronesia (federated states of)", "FM");
			nationMap.put("faroe islands", "FO");
			nationMap.put("fro", "FO");
			nationMap.put("fra", "FR");
			nationMap.put("france", "FR");
			nationMap.put("gabon", "GA");
			nationMap.put("gab", "GA");
			nationMap.put("uk", "GB");
			nationMap.put("gbr", "GB");
			nationMap.put("scotland", "GB");
			nationMap.put("wales", "GB");
			nationMap.put("united kingdom of great britain and northern ireland", "GB");
			nationMap.put("england", "GB");
			nationMap.put("united kingdom", "GB");
			nationMap.put("grd", "GD");
			nationMap.put("grenada", "GD");
			nationMap.put("georgia", "GE");
			nationMap.put("georgia (republic)", "GE");
			nationMap.put("geo", "GE");
			nationMap.put("french guiana", "GF");
			nationMap.put("guf", "GF");
			nationMap.put("guernsey", "GG");
			nationMap.put("ggy", "GG");
			nationMap.put("gha", "GH");
			nationMap.put("ghana", "GH");
			nationMap.put("gibraltar", "GI");
			nationMap.put("gib", "GI");
			nationMap.put("greenland", "GL");
			nationMap.put("grl", "GL");
			nationMap.put("gambia", "GM");
			nationMap.put("gmb", "GM");
			nationMap.put("guinea", "GN");
			nationMap.put("gin", "GN");
			nationMap.put("guadeloupe", "GP");
			nationMap.put("glp", "GP");
			nationMap.put("equatorial guinea", "GQ");
			nationMap.put("gnq", "GQ");
			nationMap.put("grc", "GR");
			nationMap.put("greece", "GR");
			nationMap.put("south georgia and the south sandwich islands", "GS");
			nationMap.put("sgs", "GS");
			nationMap.put("guatemala", "GT");
			nationMap.put("gtm", "GT");
			nationMap.put("guam", "GU");
			nationMap.put("gum", "GU");
			nationMap.put("gnb", "GW");
			nationMap.put("guinea-bissau", "GW");
			nationMap.put("guyana", "GY");
			nationMap.put("guy", "GY");
			nationMap.put("hong kong", "HK");
			nationMap.put("hkg", "HK");
			nationMap.put("heard island and mcdonald islands", "HM");
			nationMap.put("hmd", "HM");
			nationMap.put("hnd", "HN");
			nationMap.put("honduras", "HN");
			nationMap.put("hrv", "HR");
			nationMap.put("croatia", "HR");
			nationMap.put("haiti", "HT");
			nationMap.put("hti", "HT");
			nationMap.put("hun", "HU");
			nationMap.put("hungary", "HU");
			nationMap.put("idn", "ID");
			nationMap.put("indonesia", "ID");
			nationMap.put("irl", "IE");
			nationMap.put("ireland", "IE");
			nationMap.put("northern ireland", "IE");
			nationMap.put("israel", "IL");
			nationMap.put("isr", "IL");
			nationMap.put("imn", "IM");
			nationMap.put("isle of man", "IM");
			nationMap.put("india", "IN");
			nationMap.put("ind", "IN");
			nationMap.put("iot", "IO");
			nationMap.put("british indian ocean territory", "IO");
			nationMap.put("irq", "IQ");
			nationMap.put("iraq", "IQ");
			nationMap.put("irn", "IR");
			nationMap.put("iran (islamic republic of)", "IR");
			nationMap.put("iran", "IR");
			nationMap.put("isl", "IS");
			nationMap.put("iceland", "IS");
			nationMap.put("italy", "IT");
			nationMap.put("ita", "IT");
			nationMap.put("jey", "JE");
			nationMap.put("jersey", "JE");
			nationMap.put("jamaica", "JM");
			nationMap.put("jam", "JM");
			nationMap.put("jor", "JO");
			nationMap.put("jordan", "JO");
			nationMap.put("japan", "JP");
			nationMap.put("jpn", "JP");
			nationMap.put("ken", "KE");
			nationMap.put("kenya", "KE");
			nationMap.put("kgz", "KG");
			nationMap.put("kyrgyzstan", "KG");
			nationMap.put("cambodia", "KH");
			nationMap.put("khm", "KH");
			nationMap.put("kir", "KI");
			nationMap.put("kiribati", "KI");
			nationMap.put("comoros", "KM");
			nationMap.put("com", "KM");
			nationMap.put("kna", "KN");
			nationMap.put("saint kitts and nevis", "KN");
			nationMap.put("prk", "KP");
			nationMap.put("korea (democratic people's republic of)", "KP");
			nationMap.put("korea (north)", "KP");
			nationMap.put("kor", "KR");
			nationMap.put("korea (south)", "KR");
			nationMap.put("korea (republic of)", "KR");
			nationMap.put("kuwait", "KW");
			nationMap.put("kwt", "KW");
			nationMap.put("cym", "KY");
			nationMap.put("cayman islands", "KY");
			nationMap.put("kaz", "KZ");
			nationMap.put("kazakhstan", "KZ");
			nationMap.put("lao", "LA");
			nationMap.put("lao people's democratic republic", "LA");
			nationMap.put("lebanon", "LB");
			nationMap.put("lbn", "LB");
			nationMap.put("saint lucia", "LC");
			nationMap.put("lca", "LC");
			nationMap.put("lie", "LI");
			nationMap.put("liechtenstein", "LI");
			nationMap.put("lka", "LK");
			nationMap.put("sri lanka", "LK");
			nationMap.put("liberia", "LR");
			nationMap.put("lbr", "LR");
			nationMap.put("lso", "LS");
			nationMap.put("lesotho", "LS");
			nationMap.put("ltu", "LT");
			nationMap.put("lithuania", "LT");
			nationMap.put("luxembourg", "LU");
			nationMap.put("lux", "LU");
			nationMap.put("latvia", "LV");
			nationMap.put("lva", "LV");
			nationMap.put("lby", "LY");
			nationMap.put("libya", "LY");
			nationMap.put("mar", "MA");
			nationMap.put("morocco", "MA");
			nationMap.put("monaco", "MC");
			nationMap.put("mco", "MC");
			nationMap.put("moldova (republic of)", "MD");
			nationMap.put("moldova", "MD");
			nationMap.put("mda", "MD");
			nationMap.put("montenegro", "ME");
			nationMap.put("mne", "ME");
			nationMap.put("maf", "MF");
			nationMap.put("saint martin (french part)", "MF");
			nationMap.put("mdg", "MG");
			nationMap.put("madagascar", "MG");
			nationMap.put("mhl", "MH");
			nationMap.put("marshall islands", "MH");
			nationMap.put("macedonia", "MK");
			nationMap.put("north macedonia", "MK");
			nationMap.put("mkd", "MK");
			nationMap.put("mli", "ML");
			nationMap.put("mali", "ML");
			nationMap.put("mmr", "MM");
			nationMap.put("myanmar", "MM");
			nationMap.put("burma", "MM");
			nationMap.put("mongolia", "MN");
			nationMap.put("mng", "MN");
			nationMap.put("macao", "MO");
			nationMap.put("mac", "MO");
			nationMap.put("mnp", "MP");
			nationMap.put("northern mariana islands", "MP");
			nationMap.put("martinique", "MQ");
			nationMap.put("mtq", "MQ");
			nationMap.put("mrt", "MR");
			nationMap.put("mauritania", "MR");
			nationMap.put("msr", "MS");
			nationMap.put("montserrat", "MS");
			nationMap.put("malta", "MT");
			nationMap.put("mlt", "MT");
			nationMap.put("mauritius", "MU");
			nationMap.put("mus", "MU");
			nationMap.put("mdv", "MV");
			nationMap.put("maldives", "MV");
			nationMap.put("malawi", "MW");
			nationMap.put("mwi", "MW");
			nationMap.put("mex", "MX");
			nationMap.put("mexico", "MX");
			nationMap.put("malaysia", "MY");
			nationMap.put("mys", "MY");
			nationMap.put("moz", "MZ");
			nationMap.put("mozambique", "MZ");
			nationMap.put("namibia", "NA");
			nationMap.put("nam", "NA");
			nationMap.put("new caledonia", "NC");
			nationMap.put("ncl", "NC");
			nationMap.put("niger", "NE");
			nationMap.put("ner", "NE");
			nationMap.put("norfolk island", "NF");
			nationMap.put("nfk", "NF");
			nationMap.put("nigeria", "NG");
			nationMap.put("nga", "NG");
			nationMap.put("nicaragua", "NI");
			nationMap.put("nic", "NI");
			nationMap.put("nld", "NL");
			nationMap.put("netherlands", "NL");
			nationMap.put("norway", "NO");
			nationMap.put("nor", "NO");
			nationMap.put("npl", "NP");
			nationMap.put("nepal", "NP");
			nationMap.put("nauru", "NR");
			nationMap.put("nru", "NR");
			nationMap.put("niue", "NU");
			nationMap.put("niu", "NU");
			nationMap.put("new zealand", "NZ");
			nationMap.put("nzl", "NZ");
			nationMap.put("oman", "OM");
			nationMap.put("omn", "OM");
			nationMap.put("pan", "PA");
			nationMap.put("panama", "PA");
			nationMap.put("per", "PE");
			nationMap.put("peru", "PE");
			nationMap.put("french polynesia", "PF");
			nationMap.put("pyf", "PF");
			nationMap.put("papua new guinea", "PG");
			nationMap.put("png", "PG");
			nationMap.put("philippines", "PH");
			nationMap.put("phl", "PH");
			nationMap.put("pakistan", "PK");
			nationMap.put("pak", "PK");
			nationMap.put("poland", "PL");
			nationMap.put("pol", "PL");
			nationMap.put("saint pierre and miquelon", "PM");
			nationMap.put("spm", "PM");
			nationMap.put("pcn", "PN");
			nationMap.put("pitcairn", "PN");
			nationMap.put("puerto rico", "PR");
			nationMap.put("pri", "PR");
			nationMap.put("pse", "PS");
			nationMap.put("palestine, state of", "PS");
			nationMap.put("prt", "PT");
			nationMap.put("portugal", "PT");
			nationMap.put("palau", "PW");
			nationMap.put("plw", "PW");
			nationMap.put("paraguay", "PY");
			nationMap.put("pry", "PY");
			nationMap.put("qat", "QA");
			nationMap.put("qatar", "QA");
			nationMap.put("reu", "RE");
			nationMap.put("réunion", "RE");
			nationMap.put("rou", "RO");
			nationMap.put("romania", "RO");
			nationMap.put("srb", "RS");
			nationMap.put("serbia", "RS");
			nationMap.put("rus", "RU");
			nationMap.put("russia (federation)", "RU");
			nationMap.put("russian federation", "RU");
			nationMap.put("rwa", "RW");
			nationMap.put("rwanda", "RW");
			nationMap.put("sau", "SA");
			nationMap.put("saudi arabia", "SA");
			nationMap.put("solomon islands", "SB");
			nationMap.put("slb", "SB");
			nationMap.put("seychelles", "SC");
			nationMap.put("syc", "SC");
			nationMap.put("sdn", "SD");
			nationMap.put("sudan", "SD");
			nationMap.put("sweden", "SE");
			nationMap.put("swe", "SE");
			nationMap.put("singapore", "SG");
			nationMap.put("sgp", "SG");
			nationMap.put("saint helena, ascension and tristan da cunha", "SH");
			nationMap.put("shn", "SH");
			nationMap.put("svn", "SI");
			nationMap.put("slovenia", "SI");
			nationMap.put("sjm", "SJ");
			nationMap.put("svalbard and jan mayen", "SJ");
			nationMap.put("slovakia", "SK");
			nationMap.put("svk", "SK");
			nationMap.put("sle", "SL");
			nationMap.put("sierra leone", "SL");
			nationMap.put("san marino", "SM");
			nationMap.put("smr", "SM");
			nationMap.put("senegal", "SN");
			nationMap.put("sen", "SN");
			nationMap.put("somalia", "SO");
			nationMap.put("som", "SO");
			nationMap.put("suriname", "SR");
			nationMap.put("surinam", "SR");
			nationMap.put("sur", "SR");
			nationMap.put("south sudan", "SS");
			nationMap.put("ssd", "SS");
			nationMap.put("stp", "ST");
			nationMap.put("sao tome and principe", "ST");
			nationMap.put("slv", "SV");
			nationMap.put("el salvador", "SV");
			nationMap.put("sint maarten (dutch part)", "SX");
			nationMap.put("sxm", "SX");
			nationMap.put("syria", "SY");
			nationMap.put("syr", "SY");
			nationMap.put("syrian arab republic", "SY");
			nationMap.put("swz", "SZ");
			nationMap.put("swaziland", "SZ");
			nationMap.put("tca", "TC");
			nationMap.put("turks and caicos islands", "TC");
			nationMap.put("tcd", "TD");
			nationMap.put("chad", "TD");
			nationMap.put("atf", "TF");
			nationMap.put("french southern territories", "TF");
			nationMap.put("tgo", "TG");
			nationMap.put("togo", "TG");
			nationMap.put("thailand", "TH");
			nationMap.put("tha", "TH");
			nationMap.put("tajikistan", "TJ");
			nationMap.put("tjk", "TJ");
			nationMap.put("tkl", "TK");
			nationMap.put("tokelau", "TK");
			nationMap.put("tls", "TL");
			nationMap.put("timor-leste", "TL");
			nationMap.put("tkm", "TM");
			nationMap.put("turkmenistan", "TM");
			nationMap.put("tunisia", "TN");
			nationMap.put("tun", "TN");
			nationMap.put("ton", "TO");
			nationMap.put("tonga", "TO");
			nationMap.put("turkey", "TR");
			nationMap.put("tur", "TR");
			nationMap.put("tto", "TT");
			nationMap.put("trinidad and tobago", "TT");
			nationMap.put("tuv", "TV");
			nationMap.put("tuvalu", "TV");
			nationMap.put("twn", "TW");
			nationMap.put("taiwan, province of china[note 1]", "TW");
			nationMap.put("tanzania, united republic of", "TZ");
			nationMap.put("tanzania", "TZ");
			nationMap.put("tza", "TZ");
			nationMap.put("ukraine", "UA");
			nationMap.put("ukr", "UA");
			nationMap.put("uganda", "UG");
			nationMap.put("uga", "UG");
			nationMap.put("umi", "UM");
			nationMap.put("united states minor outlying islands", "UM");
			nationMap.put("united states of america", "US");
			nationMap.put("usa", "US");
			nationMap.put("united states", "US");
			nationMap.put("uruguay", "UY");
			nationMap.put("ury", "UY");
			nationMap.put("uzbekistan", "UZ");
			nationMap.put("uzb", "UZ");
			nationMap.put("vatican city", "VA");
			nationMap.put("holy see", "VA");
			nationMap.put("vat", "VA");
			nationMap.put("saint vincent and the grenadines", "VC");
			nationMap.put("vct", "VC");
			nationMap.put("venezuela", "VE");
			nationMap.put("ven", "VE");
			nationMap.put("venezuela (bolivarian republic of)", "VE");
			nationMap.put("virgin islands (british)", "VG");
			nationMap.put("british virgin islands", "VG");
			nationMap.put("vgb", "VG");
			nationMap.put("vir", "VI");
			nationMap.put("virgin islands (u.s.)", "VI");
			nationMap.put("virgin islands of the united states", "VI");
			nationMap.put("vietnam", "VN");
			nationMap.put("viet nam", "VN");
			nationMap.put("vnm", "VN");
			nationMap.put("vanuatu", "VU");
			nationMap.put("vut", "VU");
			nationMap.put("wallis and futuna", "WF");
			nationMap.put("wlf", "WF");
			nationMap.put("wsm", "WS");
			nationMap.put("samoa", "WS");
			nationMap.put("yemen", "YE");
			nationMap.put("yem", "YE");
			nationMap.put("mayotte", "YT");
			nationMap.put("myt", "YT");
			nationMap.put("south africa", "ZA");
			nationMap.put("zaf", "ZA");
			nationMap.put("zambia", "ZM");
			nationMap.put("zmb", "ZM");
			nationMap.put("zimbabwe", "ZW");
			nationMap.put("zwe", "ZW");
		}
		
		public void setup(Context context) throws IOException,
				InterruptedException {
			initMonthmonthMap();
			InitLangShortMap();
			InitNationMap();
			
		}

		public void cleanup(Context context) throws IOException,
				InterruptedException {
			
		}		
		
		
	
		//记录日志到HDFS
		public boolean log2HDFSForMapper(Context context, String text) {
			Date dt=new Date();//如果不需要格式,可直接用dt,dt就是当前系统时间
			DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");//设置显示格式
			String nowTime = df.format(dt);//用DateFormat的format()方法在dt中获取并以yyyy/MM/dd HH:mm:ss格式显示
			
			df = new SimpleDateFormat("yyyyMMdd");//设置显示格式
			String nowDate = df.format(dt);//用DateFormat的format()方法在dt中获取并以yyyy/MM/dd HH:mm:ss格式显示
			
			text = nowTime + "\n" + text + "\n\n";
			
			boolean bException = false;
			BufferedWriter out = null;
			try {
				//获取HDFS文件系统  
		        FileSystem fs = FileSystem.get(context.getConfiguration());
		        FSDataOutputStream fout = null;
		        String pathfile = "/user/dgy/log/" + nowDate + ".txt";
		        if (fs.exists(new Path(pathfile))) {
		        	fout = fs.append(new Path(pathfile));
				}
		        else {
		        	fout = fs.create(new Path(pathfile));
		        }
		        out = new BufferedWriter(new OutputStreamWriter(fout, "UTF-8"));
			    out.write(text);
			    out.close();    
			} catch (Exception ex){
				bException = true;
			}
			
			if (bException){
				return false;
			}
			else {
				return true;
			}
		}
		
		
		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			
				
			String rawid = "";
			String db = "";
			String identifier_doi="";
			String title="";
			String title_alternative="";
			String identifier_pissn="";
			String identifier_eissn="";
			String creator="";
			String creator_institution="";
			String gch="";
			String provider = "ebscocmedmjournal";
			String source="";
			String source_id="";
			String description="";
			String description_en="";
			String subject="";
			String page="";
			String beginpage="";
			String endpage="";
			String language="";
			String country="US";
			String if_html_fulltex="0";
			String if_pdf_fulltext="0";
			String ref_cnt="";
			String rawtype="";
			String volume="";
			String issue="";
			
			//其他字段
			String date_info="";
			
			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("db")) {
					db = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("doi")) {
					identifier_doi = updateItem.getValue().trim().replace('\0', ' ').replace("'", "''").trim();
				}
				if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim().replace('\0', ' ').replace("'", "''").trim();
				}
				if (updateItem.getKey().equals("eissn")) {
					identifier_eissn = updateItem.getValue().trim().replace('\0', ' ').replace("'", "''").trim();
				}
				if (updateItem.getKey().equals("issn")) {
					identifier_pissn = updateItem.getValue().trim().replace('\0', ' ').replace("'", "''").trim();
				}
				if (updateItem.getKey().equals("title_alt")) {
					title_alternative = updateItem.getValue().trim().replace('\0', ' ').replace("'", "''").trim();
				}
				if (updateItem.getKey().equals("author")) {
					creator = updateItem.getValue().trim().replace('\0', ' ').replace("'", "''").trim();
				}
				if (updateItem.getKey().equals("orange")) {
					creator_institution = updateItem.getValue().trim().replace('\0', ' ').replace("'", "''").trim();
				}
//				if (updateItem.getKey().equals("journal_raw_id")) {
//					gch = provider+"@"+updateItem.getValue().trim().replace('\0', ' ').replace("'", "''").trim();
//				}
				if (updateItem.getKey().equals("title_series")) {
					source =updateItem.getValue().trim().replace('\0', ' ').replace("'", "''");
				}
//				if (updateItem.getKey().equals("journal_raw_id")) {
//					source_id =updateItem.getValue().trim();
//				}
				if (updateItem.getKey().equals("pub_date")) {
					date_info =updateItem.getValue().trim().replace('\0', ' ').replace("'", "''");
				}
				if (updateItem.getKey().equals("abstract_")) {
					description =updateItem.getValue().trim().replace('\0', ' ').replace("'", "''");
				}
				if (updateItem.getKey().equals("abstract_alt")) {
					description_en =updateItem.getValue().trim().replace('\0', ' ').replace("'", "''");
				}
				if (updateItem.getKey().equals("keyword")) {
					subject =updateItem.getValue().trim().replace('\0', ' ').replace("'", "''");
				}
				if (updateItem.getKey().equals("page_info")) {
					page =updateItem.getValue().trim().replace('\0', ' ').replace("'", "''");
				}
				if (updateItem.getKey().equals("begin_page")) {
					beginpage =updateItem.getValue().trim().replace('\0', ' ').replace("'", "''");
				}
				if (updateItem.getKey().equals("end_page")) {
					endpage =updateItem.getValue().trim().replace('\0', ' ').replace("'", "''");
				}
				if (updateItem.getKey().equals("language")) {
					language =updateItem.getValue().trim().replace('\0', ' ').replace("'", "''");
					if (language.contains(";")) {
						String language_1="";
						for (int i = 0; i < language.split(";").length; i++) {  
							if (language.split(";")[i].length()>0) {
								if (langShort.get(language.split(";")[i].toLowerCase().trim())!=null) {
									language_1+=langShort.get(language.split(";")[i].toLowerCase().trim())+";";
								}
								}
							}
						language=language_1.replaceAll(";$", "");
					} else {
						language=langShort.get(language.toLowerCase());
					}
					if (language==null) {
						language="";
					}
					
				}
				if (updateItem.getKey().equals("country")) {
					country =nationMap.get(updateItem.getValue().trim().toLowerCase());
					if (country==null) {
						country="";
					}
				}
				if (updateItem.getKey().equals("if_html_fulltex")) {
					if_html_fulltex =updateItem.getValue().trim().replace('\0', ' ').replace("'", "''");
				}
				if (updateItem.getKey().equals("if_pdf_fulltext")) {
					if_pdf_fulltext =updateItem.getValue().trim().replace('\0', ' ').replace("'", "''");
				}
				if (updateItem.getKey().equals("ref_cnt")) {
					ref_cnt =updateItem.getValue().trim().replace('\0', ' ').replace("'", "''").trim();
				}
				if (updateItem.getKey().equals("raw_type")) {
					rawtype =updateItem.getValue().trim().replace('\0', ' ').replace("'", "''").trim();
				}
				if (updateItem.getKey().equals("vol")) {
					volume =updateItem.getValue().trim().replace('\0', ' ').replace("'", "''").trim();
				}
				if (updateItem.getKey().equals("num")) {
					issue =updateItem.getValue().trim().replace('\0', ' ').replace("'", "''").trim();
				}

			}
			
			String lngid = "";
			String rawidstring =rawid;
			if(db.equals("cmedm")){
				rawid="cmedm_"+rawid;
				lngid = VipIdEncode.getLngid("00116", rawid, false);
				context.getCounter("map", "cmedm").increment(1);
			}else{
				context.getCounter("map", db).increment(1);
				return;
			}
			
			String date="1900";
			String month ="00";
			String day = "00";
			String date_created= "";
			Pattern pattern = Pattern.compile("^\\d{4}");
			Matcher matcher = pattern.matcher(date_info);
			if(matcher.find()){
				date=matcher.group(0).trim();
				date_info=date_info.replace(date, "").trim();
			}
			pattern = Pattern.compile("[a-zA-Z-]+");
			matcher = pattern.matcher(date_info);
			if(matcher.find()){
				month=getMapValueByKey(matcher.group(0).trim());
				if (month.contains("-")) {
					month=month.substring(0, 3);
				}
				date_info=date_info.replace(matcher.group(0).trim(), "").trim();
			}
			if (date_info.length()>0 && date_info.length()<6) {
				if (date_info.contains("-")) {
					day=date_info.split("-")[0].trim();
				}else {
					day=date_info.trim();
				}
			}
			if (day.length()!=2) {
				day="00";
			}
			date_created=date+month+day;
			if (page.equals("N.PAG") ||page.equals("-1")) {
				page="";
			}
			
			String batch = (new SimpleDateFormat("yyyyMMdd")).format(new Date()) + "00";
			String type = "3";
			String medium = "2";
			String provider_id = provider + "@" + rawid;
			String provider_url = provider + "@http://search.ebscohost.com/login.aspx?direct=true&db=" + db + "&AN=" + rawidstring +"&lang=zh-cn&site=ehost-live";
			creator=creator.replaceAll("\\.$", "");
			creator_institution=creator_institution.replaceAll("\\.$", "");
			creator=creator.replaceAll("\\.$", "").replace('\0', ' ').replace("'", "''");
			creator_institution=creator_institution.replaceAll("\\.$", "").replace('\0', ' ').replace("'", "''");
			issue=issue.replaceAll("\\.$", "").replace('\0', ' ').replace("'", "''");
			page=page.replaceAll("\\.$", "").replace('\0', ' ').replace("'", "''");
			date=date.replaceAll("\\.$", "").replace('\0', ' ').replace("'", "''");
			beginpage=beginpage.replaceAll("\\.$", "").replace('\0', ' ').replace("'", "''");
			endpage=endpage.replaceAll("\\.$", "").replace('\0', ' ').replace("'", "''");
			
			String sql = "INSERT INTO modify_title_info_zt([rawid] ,[identifier_doi] ,[title] ,[title_alternative] ,[identifier_pissn] ,[identifier_eissn] ,[creator] ,[creator_institution] ,[gch] ,[provider] ,[source] ,[description] ,[description_en] ,[subject] ,[page] ,[beginpage] ,[endpage] ,[language] ,[country] ,[if_html_fulltext] ,[if_pdf_fulltext] ,[ref_cnt] ,[rawtype] ,[lngid] ,[date] ,[date_created] ,[batch] ,[type] ,[medium] ,[provider_id] ,[provider_url] ,[volume] ,[issue]) ";
			sql += " VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');";
			sql = String.format(sql,rawid ,identifier_doi ,title ,title_alternative ,identifier_pissn ,identifier_eissn ,creator ,creator_institution ,gch ,provider ,source  ,description ,description_en ,subject ,page ,beginpage ,endpage ,language ,country ,if_html_fulltex ,if_pdf_fulltext ,ref_cnt ,rawtype ,lngid ,date ,date_created ,batch ,type ,medium ,provider_id ,provider_url ,volume ,issue);
			context.getCounter("map", "count").increment(1);
			context.write(new Text(sql), NullWritable.get());			
		}
	}

}