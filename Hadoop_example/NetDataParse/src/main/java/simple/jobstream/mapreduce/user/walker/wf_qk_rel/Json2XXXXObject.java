package simple.jobstream.mapreduce.user.walker.wf_qk_rel;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class Json2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 0;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = ""; // 这个目录会被删除重建

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

		private static String batch = "";
		private static String down_date = "";
		private static String rawid = "";
		private static String ref_cnt = "";
		private static String cited_cnt = "";
		
		public void setup(Context context) throws IOException, InterruptedException {
			batch = context.getConfiguration().get("batch");
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			cnt += 1;
			if (cnt == 1) {
				System.out.println("text:" + value.toString());
			}

			{
//				batch = "";			// 已在 setup 内初始化，无需再改
				down_date = "";
				rawid = "";
				ref_cnt = "";
				cited_cnt = "";
			}

			String line = value.toString().trim();
			Gson gson = new Gson();
			Type type = new TypeToken<Map<String, JsonElement>>() {
			}.getType();
			Map<String, JsonElement> mapField = gson.fromJson(line, type);

			if (mapField.containsKey("down_date")) {
				down_date = mapField.get("down_date").getAsString();
			}
			else {
				down_date = DateTimeHelper.getNowYear() + "0101";	// 当前年1月1号
			}
			rawid = mapField.get("rawid").getAsString();

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
						cited_cnt = m.group(1)+ "@" + down_date; 
					}
				}
			}

			{
				XXXXObject xObj = new XXXXObject();
				xObj.data.put("rawid", rawid);
				xObj.data.put("batch", batch);
				xObj.data.put("down_date", down_date);
				xObj.data.put("ref_cnt", ref_cnt);
				xObj.data.put("cited_cnt", cited_cnt);
//				xObj.data.put("parse_time", (new SimpleDateFormat("yyyy-MM-dd_kk:mm:ss")).format(new Date()));

				context.getCounter("map", "count").increment(1);

				byte[] bytes = VipcloudUtil.SerializeObject(xObj);
				context.write(new Text(rawid), new BytesWritable(bytes));
			}
		}
	}
}
