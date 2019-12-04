package simple.jobstream.mapreduce.common.vip;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;

/**
 * <p>Description: 供初始解析调用 </p>  
 * @author qiuhongyang 2018年11月27日 下午3:01:34
 */
public class UniqXXXXObjectReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
	public void reduce(Text key, Iterable<BytesWritable> values, Context context)
			throws IOException, InterruptedException {

		BytesWritable bOut = new BytesWritable(); // 用于最后输出
		for (BytesWritable item : values) {
			if (item.getLength() > bOut.getLength()) { // 选最大的一个
				bOut.set(item.getBytes(), 0, item.getLength());
			}
		}
		
		// 检查 down_date 和 batch
		{
			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(bOut.getBytes(), xObj);
			String down_date = "";
			String batch = "";
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("down_date")) {
					down_date = updateItem.getValue().trim();
				}  else if (updateItem.getKey().equals("batch")) {
					batch = updateItem.getValue().trim();
				}
			}
				
			if (!DateTimeHelper.checkDownDate(down_date)) {
				context.getCounter("reduce", "err down_date").increment(1);
			}
			
			context.getCounter("reduce", "batch:" + batch).increment(1);
			if (!DateTimeHelper.checkParseTime(batch)) {
				context.getCounter("reduce", "err batch").increment(1);
			}
		}

		context.getCounter("reduce", "count").increment(1);

		bOut.setCapacity(bOut.getLength()); // 将buffer设为实际长度

		context.write(key, bOut);
	}
}
