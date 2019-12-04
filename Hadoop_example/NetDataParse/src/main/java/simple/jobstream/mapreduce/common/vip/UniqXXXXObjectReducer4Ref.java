package simple.jobstream.mapreduce.common.vip;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;

/**
 * <p>
 * Description: 供初始解析调用，单批引文去重（refer_text_site）
 * </p>
 * 
 * @author qiuhongyang 2018年11月27日 下午3:01:34
 */
public class UniqXXXXObjectReducer4Ref extends Reducer<Text, BytesWritable, Text, BytesWritable> {
	public void reduce(Text key, Iterable<BytesWritable> values, Context context)
			throws IOException, InterruptedException {
		Set<String> refertextSet = new HashSet<String>();

		int idx = 0;
		for (BytesWritable item : values) {
			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(item.getBytes(), xObj);

			// 下载日期
			if (!xObj.data.containsKey("down_date")) {		//下载日期不存在
				context.getCounter("reduce", "err no down_date").increment(1);
				continue;
			}
			String down_date = xObj.data.get("down_date").trim();
			if (!DateTimeHelper.checkDownDate(down_date)) {		// 下载日期不合规范
				context.getCounter("reduce", "err down_date").increment(1);
				continue;
			}

			// 批次号（解析时间）
			if (!xObj.data.containsKey("batch")) {		//解析时间不存在
				context.getCounter("reduce", "err no batch").increment(1);
				continue;
			}
			String batch = xObj.data.get("batch").trim();
			context.getCounter("reduce", "batch:" + batch).increment(1);
			if (!DateTimeHelper.checkParseTime(batch)) {	//解析时间不合规范
				context.getCounter("reduce", "err batch").increment(1);
			}
			
			// 主题录ID（编号后ID@原始ID）
			if (!xObj.data.containsKey("cited_id")) {		//主题录ID不存在
				context.getCounter("reduce", "err no cited_id").increment(1);
				continue;
			}
			String cited_id = xObj.data.get("cited_id").trim();
			String[] vec = cited_id.split("@");
			if (vec.length != 2) {
				context.getCounter("reduce", "err cited_id").increment(1);
				continue;
			}
			cited_id = vec[0].trim();
			

			// 引文文本
			if (!xObj.data.containsKey("refer_text_site")) {
				context.getCounter("reduce", "err no refer_text_site").increment(1);
				continue;
			}
			String refer_text_site = xObj.data.get("refer_text_site").trim();
			if (refer_text_site.length() < 1) {
				context.getCounter("reduce", "err blank refer_text_site").increment(1);
				continue;
			}
			if (refertextSet.contains(refer_text_site)) { // 排除重复引文
				context.getCounter("reduce", "repeat ref").increment(1);
				continue;
			}
			refertextSet.add(refer_text_site);
			++idx;
			String lngid = cited_id + String.format("%04d", idx);
			xObj.data.put("lngid", lngid);
			
			context.getCounter("reduce", "count").increment(1);
			
			byte[] bytes = VipcloudUtil.SerializeObject(xObj);			
			context.write(key, new BytesWritable(bytes)); // 输出无重复的引文
		}
	}
}
