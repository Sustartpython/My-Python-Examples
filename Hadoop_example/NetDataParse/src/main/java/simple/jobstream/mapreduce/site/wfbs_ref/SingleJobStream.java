package simple.jobstream.mapreduce.site.wfbs_ref;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";

		String rawHtmlDir = "/RawData/wanfang/qk/ref/big_json/20190122";
		String rawXXXXObjectDir = "/RawData/wanfang/qk/ref/XXXXObject";
		String latest_tempDir = "/RawData/wanfang/qk/ref/latest_temp"; // 临时成品目录
		String latestDir = "/RawData/wanfang/qk/ref/latest"; // 成品目录
		String latestExportDir = "/RawData/wanfang/qk/ref/latest_export"; // 成品db3目录
		String newXXXXObjectDir = "/RawData/wanfang/qk/ref/new_data/XXXXObject"; // 本次新增的数据（XXXXObject）
		String stdDir = "/RawData/wanfang/qk/ref/new_data/StdWFQKRef";

		JobNode Html2XXXXObject = new JobNode("WFQKRefParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.wf_qk_ref.Json2XXXXObject");
		Html2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
		Html2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);

		// 将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp = new JobNode("WFQKRefParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.wf_qk_ref.MergeXXXXObject2Temp");
		MergeXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
		MergeXXXXObject2Temp.setConfig("outputHdfsPath", latest_tempDir);

		// 生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = new JobNode("WFQKRefParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.wf_qk_ref.GenNewData");
		GenNewData.setConfig("inputHdfsPath", latest_tempDir + "," + rawXXXXObjectDir);
		GenNewData.setConfig("outputHdfsPath", newXXXXObjectDir);

		JobNode StdWFQKRef = new JobNode("WFQKRefParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.wf_qk_ref.StdWFQKRef");
		StdWFQKRef.setConfig("inputHdfsPath", newXXXXObjectDir);
		StdWFQKRef.setConfig("outputHdfsPath", stdDir);
		StdWFQKRef.setConfig("reduceNum", "1");

		// 备份累积数据
		JobNode Temp2Latest = new JobNode("WFQKRefParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.wf_qk_ref.Temp2Latest");
		Temp2Latest.setConfig("inputHdfsPath", latest_tempDir);
		Temp2Latest.setConfig("outputHdfsPath", latestDir);

		// 导入latest里面的完整数据到db3
		JobNode ExportLatest = new JobNode("WFQKRefParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.wf_qk_ref.ExportLatest");
		ExportLatest.setConfig("inputHdfsPath", latestDir);
		ExportLatest.setConfig("outputHdfsPath", latestExportDir);

		// 正常更新
		result.add(Html2XXXXObject);
		Html2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdWFQKRef);
		StdWFQKRef.addChildJob(Temp2Latest);
		

		return result;
	}
}
