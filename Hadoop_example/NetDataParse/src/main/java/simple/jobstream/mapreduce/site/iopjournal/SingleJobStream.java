package simple.jobstream.mapreduce.site.iopjournal;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();

		String rawJsonDir = "/RawData/iopjournal/big_json/20190701";
		String rawXXXXObjectDir = "/RawData/iopjournal/XXXXObject";
		String latest_tempDir = "/RawData/iopjournal/latest_temp"; // 临时成品目录
		String latestDir = "/RawData/iopjournal/latest"; // 成品目录
		String newXXXXObjectDir = "/RawData/iopjournal/new_data/XXXXObject"; // 本次新增的数据（XXXXObject）
		String stdDir = "/RawData/iopjournal/new_data/StdDb3";
		
		JobNode Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("iopjournal.Json2XXXXObject", 
				DateTimeHelper.getNowTimeAsBatch(),
				"simple.jobstream.mapreduce.site.iopjournal.Json2XXXXObject", 
				rawJsonDir, 
				rawXXXXObjectDir, 
				10);

		// 将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp = JobNodeModel.getJonNode4MergeXXXXObject("iopjournal.MergeXXXXObject2Temp",
				rawXXXXObjectDir, latestDir, latest_tempDir, 20);

		// 生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("iopjournal.GenNewData", 
				rawXXXXObjectDir, latest_tempDir, newXXXXObjectDir, 10);	

		// 导出数据到 db3
		JobNode Std2Db3 = JobNodeModel.getJobNode4Std2Db3("iopjournal.Std2Db3", 
				"simple.jobstream.mapreduce.site.iopjournal.Std2Db3", 
				newXXXXObjectDir, 
				stdDir, 
				"iopjournal", 
				"/RawData/_rel_file/zt_template.db3", 
				1);

		// 备份累积数据
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("iopjournal.Temp2Latest", latest_tempDir, latestDir);

		// 正常更新
		result.add(Json2XXXXObject);
		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(Std2Db3);
		Std2Db3.addChildJob(Temp2Latest);

		// 单次测试
//		result.add(Json2XXXXObject);
//		Std2DB3.setConfig("inputHdfsPath", rawXXXXObjectDir);
//		Json2XXXXObject.addChildJob(Std2DB3);


		return result;
	}
}
