package simple.jobstream.mapreduce.site.sd_qk;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();

		String rawHtmlDir = "/RawData/elsevier/sd_qk/big_json/20190524";
		String rawXXXXObjectDir = "/RawData/elsevier/sd_qk/XXXXObject";
		String latest_tempDir = "/RawData/elsevier/sd_qk/latest_temp"; // 临时成品目录
		String latestDir = "/RawData/elsevier/sd_qk/latest"; // 成品目录
		String newXXXXObjectDir = "/RawData/elsevier/sd_qk/new_data/XXXXObject"; // 本次新增的数据（XXXXObject）
		String stdDir = "/RawData/elsevier/sd_qk/new_data/StdSDQK";
		
		JobNode Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("sd_qk.Json2XXXXObject", 
				DateTimeHelper.getNowTimeAsBatch(), 
				"simple.jobstream.mapreduce.site.sd_qk.Json2XXXXObject", 
				rawHtmlDir, 
				rawXXXXObjectDir, 
				10);

		// 将历史累积数据和新数据合并去重	
		JobNode MergeXXXXObject2Temp = JobNodeModel.getJonNode4MergeXXXXObject("sd_qk.MergeXXXXObject2Temp",
				rawXXXXObjectDir, latestDir, latest_tempDir, 100);

		// 生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("sd_qk.GenNewData", 
				rawXXXXObjectDir, latest_tempDir, newXXXXObjectDir, 50);		


		// 导出数据到 db3
		JobNode Std2Db3 = JobNodeModel.getJobNode4Std2Db3("sd_qk.Std2Db3", 
				"simple.jobstream.mapreduce.site.sd_qk.Std2Db3", 
				newXXXXObjectDir, 
				stdDir, 
				"sciencedirectjournal", 
				"/RawData/_rel_file/zt_template.db3", 
				1);

		// 备份累积数据
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("sd_qk.Temp2Latest", latest_tempDir, latestDir);

		// 正常更新
//		result.add(Json2XXXXObject);
//		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
//		MergeXXXXObject2Temp.addChildJob(GenNewData);
//		GenNewData.addChildJob(Std2Db3);
//		Std2Db3.addChildJob(Temp2Latest);
		
///*************************************************************************************/
// 过滤导出
		// 以智立方格式导出到db3
		JobNode Std2Db3Filter = JobNodeModel.getJobNode4Std2Db3("sd_qk.Std2Db3", 
													"simple.jobstream.mapreduce.site.sd_qk.Std2Db3Filter", 
													latestDir, 
													"/RawData/elsevier/sd_qk/new_data/Std2Db3Filter", 
													"sciencedirectjournal", 
													"/RawData/_rel_file/zt_template.db3", 
													1);
		result.add(Std2Db3Filter);

		// 单轮测试
//		result.add(Json2XXXXObject);
//		Std2Db3.setConfig("inputHdfsPath", rawXXXXObjectDir);
//		Json2XXXXObject.addChildJob(Std2Db3);

		// 导出所有
//		StdSDQK.setConfig("inputHdfsPath", latestDir);
//		StdSDQK.setConfig("reduceNum", "20");
//		result.add(StdSDQK);


		return result;
	}
}
