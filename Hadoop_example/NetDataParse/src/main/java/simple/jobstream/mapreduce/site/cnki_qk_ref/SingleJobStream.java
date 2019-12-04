package simple.jobstream.mapreduce.site.cnki_qk_ref;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";

		String rawHtmlDir = "/RawData/cnki/qk/ref/big_json/20190124";
		String rawXXXXObjectDir = "/RawData/cnki/qk/ref/XXXXObject";
		String latest_tempDir = "/RawData/cnki/qk/ref/latest_temp"; 	// 临时成品目录
		String latestDir = "/RawData/cnki/qk/ref/latest"; 				// 成品目录
		String latestExportDir = "/RawData/cnki/qk/ref/latest_export"; 	// 成品db3目录
		String newXXXXObjectDir = "/RawData/cnki/qk/ref/new_data/XXXXObject"; // 本次新增的数据（XXXXObject）
		String stdDir = "/RawData/cnki/qk/ref/new_data/StdCNKIQKRef";

		/**/
		JobNode Html2XXXXObject = new JobNode("CNKIQKRefParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.cnki_qk_ref.Html2XXXXObject");
		Html2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
		Html2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);

		// 将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp = new JobNode("CNKIQKRefParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.cnki_qk_ref.MergeXXXXObject2Temp");
		MergeXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
		MergeXXXXObject2Temp.setConfig("outputHdfsPath", latest_tempDir);

		// 生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = new JobNode("CNKIQKRefParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.cnki_qk_ref.GenNewData");
		GenNewData.setConfig("inputHdfsPath", latest_tempDir + "," + rawXXXXObjectDir);
		GenNewData.setConfig("outputHdfsPath", newXXXXObjectDir);

		JobNode StdCNKIQKRef = new JobNode("CNKIQKRefParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.cnki_qk_ref.StdCNKIQKRef");
		StdCNKIQKRef.setConfig("inputHdfsPath", newXXXXObjectDir);
		StdCNKIQKRef.setConfig("outputHdfsPath", stdDir);
		StdCNKIQKRef.setConfig("reduceNum", "1");

		// 备份累积数据
		JobNode Temp2Latest = new JobNode("CNKIQKRefParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.cnki_qk_ref.Temp2Latest");
		Temp2Latest.setConfig("inputHdfsPath", latest_tempDir);
		Temp2Latest.setConfig("outputHdfsPath", latestDir);

		// 导入latest里面的完整数据到db3
		JobNode ExportLatest = new JobNode("CNKIQKRefParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.cnki_qk_ref.ExportLatest");
		ExportLatest.setConfig("inputHdfsPath", latestDir);
		ExportLatest.setConfig("outputHdfsPath", latestExportDir);

		// 正常更新
		result.add(Html2XXXXObject);
		Html2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdCNKIQKRef);
		StdCNKIQKRef.addChildJob(Temp2Latest);

//		result.add(Html2XXXXObject);
//		StdCNKIQKRef.setConfig("inputHdfsPath", rawXXXXObjectDir);
//		Html2XXXXObject.addChildJob(StdCNKIQKRef);

		return result;
	}
}
