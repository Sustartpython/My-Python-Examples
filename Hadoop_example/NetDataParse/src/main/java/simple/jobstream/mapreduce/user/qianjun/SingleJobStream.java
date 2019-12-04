package simple.jobstream.mapreduce.user.qianjun;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String rootDir = "";

		// 第一步：将最近增加的big_htm文件转换为xxxxobjcet文件存放在xxxxobject目录下
		  JobNode wordcount = new JobNode("wordcount", rootDir, 0,
				    "simple.jobstream.mapreduce.user.qianjun.WordCount");
		// 正常更新
		result.add(wordcount);

		return result;
	}

}
