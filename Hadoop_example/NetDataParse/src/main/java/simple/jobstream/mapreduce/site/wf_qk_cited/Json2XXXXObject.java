package simple.jobstream.mapreduce.site.wf_qk_cited;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class Json2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 100;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = ""; // 这个目录会被删除重建

	public void pre(Job job) {
		String jobName = "wf_qk_cited." + this.getClass().getSimpleName();

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

		static int cnt = 0;

		private static String rawid = "";
		private static String ref_cnt = "";
		private static String cited_cnt = "";

		// 获取 json 值，可能为一个或多个或没有此键
		String getJsonValue(JsonObject articleJsonObject, String jsonKey) {
			String line = "";

			JsonElement jsonValueElement = articleJsonObject.get(jsonKey);
			if ((null == jsonValueElement) || jsonValueElement.isJsonNull()) {
				line = "";
			} else if (jsonValueElement.isJsonArray()) {
				for (JsonElement jEle : jsonValueElement.getAsJsonArray()) {
					line += jEle.getAsString().trim() + ";";
				}
			} else {
				line = jsonValueElement.getAsString().trim();
			}
			line = line.replaceAll(";$", ""); // 去掉末尾的分号
			line = line.trim();

			return line;
		}

		public boolean parseArticle(JsonElement articleJsonElement) {
			{
				rawid = "";
				ref_cnt = "";
				cited_cnt = "";
			}

			JsonObject articleJsonObject = articleJsonElement.getAsJsonObject();

			rawid = getJsonValue(articleJsonObject, "article_id");

			ref_cnt = getJsonValue(articleJsonObject, "refdoc_cnt"); // 与详情页不一致，靠不住
			cited_cnt = getJsonValue(articleJsonObject, "cited_cnt"); // 与详情页不一致，靠不住

			// System.out.println("article_id: " + rawid);
			// System.out.println("pykm: " + pykm);
			// System.out.println("title_c: " + title_c);
			// System.out.println("title_e: " + title_e);
			// System.out.println("remark_c: " + remark_c);
			// System.out.println("remark_e: " + remark_e);
			// System.out.println("doi: " + doi);
			// System.out.println("author_c: " + author_c);
			// System.out.println("author_e: " + author_e);
			// System.out.println("organ: " + organ);
			// System.out.println("name_c: " + name_c);
			// System.out.println("name_e: " + name_e);
			// System.out.println("years: " + years);
			// System.out.println("vol: " + vol);
			// System.out.println("num: " + num);
			// System.out.println("sClass: " + sClass);
			// System.out.println("auto_class: " + auto_class);
			// System.out.println("keyword_c: " + keyword_c);
			// System.out.println("keyword_e: " + keyword_e);
			// System.out.println("imburse: " + imburse);
			// System.out.println("pageline: " + pageline);
			// System.out.println("muinfo: " + muinfo);
			// System.out.println("pub1st: " + pub1st);
			// System.out.println("src_db: " + src_db);
			// System.out.println("*****************************\n");

			return true;
		}

		// 记录日志到HDFS
		public boolean log2HDFSForMapper(Context context, String text) {
			Date dt = new Date();// 如果不需要格式,可直接用dt,dt就是当前系统时间
			DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");// 设置显示格式
			String nowTime = df.format(dt);// 用DateFormat的format()方法在dt中获取并以yyyy/MM/dd
											// HH:mm:ss格式显示

			df = new SimpleDateFormat("yyyyMMdd");// 设置显示格式
			String nowDate = df.format(dt);// 用DateFormat的format()方法在dt中获取并以yyyy/MM/dd
											// HH:mm:ss格式显示

			text = nowTime + "\n" + text + "\n\n";

			boolean bException = false;
			BufferedWriter out = null;
			try {
				// 获取HDFS文件系统
				FileSystem fs = FileSystem.get(context.getConfiguration());

				FSDataOutputStream fout = null;
				String pathfile = "/user/qhy/log/log_map/" + nowDate + ".txt";
				if (fs.exists(new Path(pathfile))) {
					fout = fs.append(new Path(pathfile));
				} else {
					fout = fs.create(new Path(pathfile));
				}

				out = new BufferedWriter(new OutputStreamWriter(fout, "UTF-8"));
				out.write(text);
				out.close();

			} catch (Exception ex) {
				bException = true;
				ex.printStackTrace();
			}

			if (bException) {
				return false;
			} else {
				return true;
			}
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			cnt += 1;
			if (cnt == 1) {
				System.out.println("text:" + value.toString());
			}

			{
				rawid = "";
				ref_cnt = "";
				cited_cnt = "";
			}

			String line = value.toString().trim();
			Gson gson = new Gson();
			Type type = new TypeToken<Map<String, JsonElement>>() {
			}.getType();
			Map<String, JsonElement> mapField = gson.fromJson(line, type);

			rawid = mapField.get("article_id").getAsString();

			JsonArray nodesJsonArray = mapField.get("nodes").getAsJsonArray();

			for (JsonElement eleNode : nodesJsonArray) {
				// 如果 category 不为 1
				if (!eleNode.getAsJsonObject().get("category").getAsString().equals("1")) {
					continue;
				}

				String label = eleNode.getAsJsonObject().get("label").getAsString();
				if (eleNode.getAsJsonObject().get("name").getAsString().equals("参考文献")) {
					// "label": "参考文献(3)"
					// 创建 Pattern 对象
					Pattern r = Pattern.compile("参考文献\\((\\d+)\\)");
					// 创建 matcher 对象
					Matcher m = r.matcher(label);
					if (m.find()) {
						ref_cnt = m.group(1);
					}
				} else if (eleNode.getAsJsonObject().get("name").getAsString().equals("引证文献")) {
					// "label": "引证文献(1)"
					// 创建 Pattern 对象
					Pattern r = Pattern.compile("引证文献\\((\\d+)\\)");
					// 创建 matcher 对象
					Matcher m = r.matcher(label);
					if (m.find()) {
						cited_cnt = m.group(1);
					}
				}
			}

			{
				XXXXObject xObj = new XXXXObject();
				xObj.data.put("rawid", rawid);
				xObj.data.put("ref_cnt", ref_cnt);
				xObj.data.put("cited_cnt", cited_cnt);
				xObj.data.put("parse_time", (new SimpleDateFormat("yyyy-MM-dd_kk:mm:ss")).format(new Date()));

				context.getCounter("map", "count").increment(1);

				byte[] bytes = VipcloudUtil.SerializeObject(xObj);
				context.write(new Text(rawid), new BytesWritable(bytes));
			}
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
