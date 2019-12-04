package simple.jobstream.mapreduce.site.cnkibs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.json.JSONArray;
import org.json.JSONObject;

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.vip.VipIdEncode;
import simple.jobstream.mapreduce.common.util.StringHelper;
// 将A层xxxx转成智立方xxxx

// 将以往数据转为A层格式
public class AXXXXObiectForZlF extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 100;

	public static String inputHdfsPath = "/RawData/cnki/bs/latest";
	public static String outputHdfsPath = "/RawData/cnki/bs/XXXXObject_ZLF";

	public void pre(Job job) {
		String jobName = "cnki_bs." + this.getClass().getSimpleName();
		job.setJobName(jobName);

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
		job.setMapperClass(ProcessMapper.class);
		//job.setReducerClass(ProcessReducer.class);
		
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(BytesWritable.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(BytesWritable.class);
	    
	    //job.setInputFormatClass(SimpleTextInputFormat.class);
	    //job.setOutputFormatClass(TextOutputFormat.class);
	    
	    
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

	public static class ProcessMapper extends Mapper<Text, BytesWritable, Text, BytesWritable> {
		// 老字段
		private static String lngid = "";
		private static String rawid = "";
		private static String title = "";
		private static String subject_clc = "";
		private static String subject = "";
		private static String creator = "";
		private static String creator_descipline = "";
		private static String date = "";
		private static String creator_degree = "";// 学位
		private static String creator_institution = "";// 学校
		private static String description = "";
		private static String contributor = "";
		private static String language = "1";
		private static String country = "CN";
		private static String type = "2";
		private static String medium = "";
		private static String batch = "";
		private static String description_fund = "";
		private static ClassType classtype = null;
		private static HashMap<String, String> FirstClassMap = new HashMap<>();
		private static HashMap<String, String> SecondOnlyClassMap = new HashMap<>();
		private static HashMap<String, String> SecondClassMap = new HashMap<>();
		public void setup(Context context) throws IOException, InterruptedException {
			String firstclass_info = "/RawData/_rel_file/FirstClass.txt";
			String secondclass_info = "/RawData/_rel_file/SecondClass.txt";
			ClassLoad classload = new ClassLoad(context, firstclass_info, secondclass_info);

			FirstClassMap = classload.getfirstclass();
			SecondOnlyClassMap = classload.getsecondonlyclass();
			SecondClassMap = classload.getsecondclass();

			classtype = new ClassType(FirstClassMap, SecondClassMap, SecondOnlyClassMap);

		}

		public static JSONObject createNetFullTextAddr_All(String addr) {
			String[] attr = { addr };
			String key = "CNKI@CNKIDATA";
			HashMap<String, JSONArray> map = new HashMap<String, JSONArray>();
			try {
				JSONArray array = new JSONArray(attr);
				map.put(key, array);
				JSONObject o = new JSONObject(map);

				return o;
			} catch (Exception e) {
				return null;
			}
		}

		public static JSONObject createnetfulltextaddr_all_std(String rawid) {
			String[] attr = { rawid };
			String key = "CNKI@CNKIDATA";
			HashMap<String, JSONArray> map = new HashMap<String, JSONArray>();
			try {
				JSONArray array = new JSONArray(attr);
				map.put(key, array);
				JSONObject o = new JSONObject(map);
				return o;
			} catch (Exception e) {
				return null;
			}
		}
		
		// 重新生成batch
		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
			{// 老字段
				lngid = "";
				rawid = "";
				title = "";
				subject_clc = "";
				subject = "";
				creator = "";
				creator_descipline = "";
				date = "";
				creator_degree = "";// 学位
				creator_institution = "";// 学校
				description = "";
				contributor = "";
				language = "1";
				country = "CN";
				type = "2";
				medium = "";
				batch = "";
				description_fund = "";
			}

			XXXXObject xObj = new XXXXObject();
			byte[] bs = new byte[value.getLength()];  
			System.arraycopy(value.getBytes(), 0, bs, 0, value.getLength());  
			VipcloudUtil.DeserializeObject(bs, xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("lngid")) {
					lngid = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("author")) {
					creator = updateItem.getValue().trim();
				}

				if (updateItem.getKey().equals("degree")) {
					creator_degree = updateItem.getValue().trim();
				}

				if (updateItem.getKey().equals("subject_dsa")) {
					creator_descipline = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("organ")) {
					creator_institution = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("contributor")) {
					contributor = updateItem.getValue().trim();
				}

				if (updateItem.getKey().equals("abstract")) {
					description = updateItem.getValue().trim();
				}

				if (updateItem.getKey().equals("subject")) {
					subject = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("clc_no")) {
					subject_clc = updateItem.getValue().trim();
				}

				if (updateItem.getKey().equals("pub_year")) {
					date = updateItem.getValue().trim();
				}

				if (updateItem.getKey().equals("fund")) {
					description_fund = updateItem.getValue().trim();
				}

			}
				
			
			if (title.length() < 2) {
				return;
			}
//			title = title.replace('\0', ' ').replace("'", "''").trim();
//			contributor = contributor.replace("'", "''").trim();
//			creator = creator.replace('\0', ' ').replace("'", "''").trim();
//			creator_descipline = creator_descipline.replace('\0', ' ').replace("'", "''").trim();
//			creator_institution = creator_institution.replace('\0', ' ').replace("'", "''").trim();
//			description = description.replace('\0', ' ').replace("'", "''").trim();
//			description_fund = description_fund.replace('\0', ' ').replace("'", "''").trim();
//			subject_clc = subject_clc.replace('\0', ' ').replace("'", "''").replace(" ", ";").replace(",", ";").trim();
//			subject = subject.replace('\0', ' ').replace("'", "''").trim().replace(";;", ";");
//		
			subject_clc = subject_clc.replace(",", ";").trim();
			subject = subject.replace(";;", ";").trim();
			if (description_fund.endsWith(";")) {
				description_fund = description_fund.substring(0, description_fund.length() - 1);
			}
//			batch = (new SimpleDateFormat("yyyyMMdd")).format(new Date()) + "00";
			String NetFullTextAddr = "http://epub.cnki.net/kns/detail/detail.aspx?dbname=CMFD&filename=" + rawid;

			// String owner = "cqu";
			String titletype = "0;1;512;513";
			String srcID = "VIP";
			String srcproducer = "CNKI";
			JSONObject NetFullTextAddr_All = createNetFullTextAddr_All(NetFullTextAddr);

			JSONObject netfulltextaddr_all_std = createnetfulltextaddr_all_std(rawid);
			String classtypes = classtype.GetClassTypes(subject_clc);
			String showclasstypes = classtype.GetShowClassTypes(subject_clc);
			
			// 去除关键字中多余的;号
			subject =StringHelper.cleanSemicolon(subject);
			XXXXObject xObjOut = new XXXXObject();
			{
				xObjOut.data.put("lngid", VipIdEncode.getLngid("00075", rawid, false));
				xObjOut.data.put("country",country);
				xObjOut.data.put("language",language);
				xObjOut.data.put("showclasstypes",showclasstypes);
				xObjOut.data.put("classtypes",classtypes); 
				xObjOut.data.put("netfulltextaddr_all_std",netfulltextaddr_all_std.toString()); 
				xObjOut.data.put("NetFullTextAddr_All",NetFullTextAddr_All.toString()); 
				xObjOut.data.put("srcID",srcID); 
				xObjOut.data.put("titletype",titletype); 
				xObjOut.data.put("srcproducer" ,srcproducer); 
				xObjOut.data.put("lngid",lngid); 
				xObjOut.data.put("rawid" ,rawid); 
				xObjOut.data.put("title_c", title); 
				xObjOut.data.put("Showwriter" ,creator); 
				xObjOut.data.put("bsdegree" ,creator_degree); 
				xObjOut.data.put("bsspeciality", creator_descipline); 
				xObjOut.data.put("Showorgan" ,creator_institution); 
				xObjOut.data.put("bstutorsname ", contributor); 
				xObjOut.data.put("remark_c",description); 
				xObjOut.data.put("class",subject_clc); 
				xObjOut.data.put("keyword_c",subject); 
				xObjOut.data.put("type",type); 
				xObjOut.data.put("medium",medium); 
				xObjOut.data.put("years",date); 
				xObjOut.data.put("Imburse",description_fund); 
				xObjOut.data.put("batch",batch); 
				xObjOut.data.put("netfulltextaddr",NetFullTextAddr);
			}
			context.getCounter("map", "count").increment(1);

			byte[] bytes = VipcloudUtil.SerializeObject(xObjOut);
			context.write(new Text(rawid), new BytesWritable(bytes));
		}
	}
	public static class ProcessReducer extends
			Reducer<Text, BytesWritable, Text, BytesWritable> {

		public void reduce(Text key, Iterable<BytesWritable> values, Context context)
				throws IOException, InterruptedException {

			BytesWritable bOut = new BytesWritable();
			for (BytesWritable item : values) {
				if (item.getLength() > bOut.getLength()) { // 选最大的一个
					bOut.set(item.getBytes(), 0, item.getLength());
				}
			}

			context.getCounter("reducer", "count").increment(1);
			bOut.setCapacity(bOut.getLength()); // 将buffer设为实际长度
			context.write(key, bOut);
		}
	}
}
