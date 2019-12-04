package simple.jobstream.mapreduce.site.wf_qk_cited;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";

		String rawHtmlDir = "/RawData/wanfang/qk/cited/big_json/20190122";
		String rawXXXXObjectDir = "/RawData/wanfang/qk/cited/XXXXObject";
		String latest_tempDir = "/RawData/wanfang/qk/cited/latest_temp"; // 临时成品目录
		String latestDir = "/RawData/wanfang/qk/cited/latest"; // 成品目录

		JobNode Json2XXXXObject = new JobNode("WFQKParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.wf_qk_cited.Json2XXXXObject");
		Json2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
		Json2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);

		// 将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp = new JobNode("WFQKParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.wf_qk_cited.MergeXXXXObject2Temp");
		MergeXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
		MergeXXXXObject2Temp.setConfig("outputHdfsPath", latest_tempDir);

		// 备份累积数据
		JobNode Temp2Latest = new JobNode("WFQKParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.wf_qk_cited.Temp2Latest");
		Temp2Latest.setConfig("inputHdfsPath", latest_tempDir);
		Temp2Latest.setConfig("outputHdfsPath", latestDir);

		// 正常更新
		result.add(Json2XXXXObject);
		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(Temp2Latest);

		return result;
	}
}
