package simple.jobstream.mapreduce.site.sinomedzhjournal;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";

		String rawHtmlDir = "/RawData/SinoMed/zh/big_json/2019/20191126";
		String rawXXXXObjectDir = "/RawData/SinoMed/zh/XXXXObject";
		String latest_tempDir = "/RawData/SinoMed/zh/latest_temp"; // 临时成品目录
		String latestDir = "/RawData/SinoMed/zh/latest"; // 成品目录
		String newXXXXObjectDir = "/RawData/SinoMed/zh/new_data/XXXXObject"; // 本次新增的数据（XXXXObject）
		String stdDir2 = "/RawData/SinoMed/zh/zt";
		String stdDir = "/RawData/SinoMed/zh/a";

		JobNode Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("sinomed_zh.Parse",
				DateTimeHelper.getNowTimeAsBatch(), "simple.jobstream.mapreduce.site.sinomedzhjournal.Json2XXXXObject",
				rawHtmlDir, rawXXXXObjectDir, 10);

		JobNode StdNew = JobNodeModel.getJobNode4Std2Db3("sinomed_zh.Std",
				"simple.jobstream.mapreduce.site.sinomedzhjournal.Stdzh", rawXXXXObjectDir, stdDir2, "sinomed_zh",
				"/RawData/_rel_file/zt_template.db3", 1);

		JobNode First2Latest = JobNodeModel.getJobNode4CopyXXXXObject("sinomed_zh.Copy", rawXXXXObjectDir, latestDir);

		JobNode MergeXXXXObject2Temp = JobNodeModel.getJonNode4MergeXXXXObject("sinomed_zh.Merge", rawXXXXObjectDir,
				latestDir, latest_tempDir, 10);

		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("sinomed_zh.Extract", rawXXXXObjectDir,
				latest_tempDir, newXXXXObjectDir, 10);

		JobNode StdDb3 = JobNodeModel.getJobNode4Std2Db3("sinomed_zh.Std",
				"simple.jobstream.mapreduce.site.sinomedzhjournal.Stdzh", latestDir, stdDir2, "sinomed_zh",
				"/RawData/_rel_file/zt_template.db3", 1);

		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("sinomed_zh.Copy", latest_tempDir, latestDir);

		// *
		// 正常更新

//		result.add(Json2XXXXObject);
//		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
//		MergeXXXXObject2Temp.addChildJob(GenNewData);
//		GenNewData.addChildJob(StdDb3);
//		StdDb3.addChildJob(Temp2Latest);

//		第一次上传
		result.add(Json2XXXXObject);
		Json2XXXXObject.addChildJob(First2Latest);
		First2Latest.addChildJob(StdDb3);

		return result;
	}
}
