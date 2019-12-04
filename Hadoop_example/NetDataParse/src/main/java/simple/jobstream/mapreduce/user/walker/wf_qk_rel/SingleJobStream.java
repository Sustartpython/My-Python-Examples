package simple.jobstream.mapreduce.user.walker.wf_qk_rel;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

// 万方引文量、被引量
public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();

		String rawJsonDir = "/RawData/wanfang/qk/rel/big_json/20190709";
		String rawXXXXObjectDir = "/RawData/wanfang/qk/rel/XXXXObject";
		String latestTempDir = "/RawData/wanfang/qk/rel/latest_temp"; // 临时成品目录
		String latestDir = "/RawData/wanfang/qk/rel/latest"; // 成品目录

		JobNode Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("wf_qk_rel.Json2XXXXObject", 
												DateTimeHelper.getNowTimeAsBatch(), 
												"simple.jobstream.mapreduce.user.walker.wf_qk_rel.Json2XXXXObject", 
												rawJsonDir, 
												rawXXXXObjectDir, 
												100);

		// 将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp = JobNodeModel.getJonNode4MergeXXXXObjectMergeDateDynamicValue("wf_qk_rel.MergeXXXXObject2Temp", 
														rawXXXXObjectDir, 
														latestDir, 
														latestTempDir, 
														200);

		// 备份累积数据
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("wf_qk_rel.Temp2Latest", 
											latestTempDir, 
											latestDir);

		// 正常更新
		result.add(Json2XXXXObject);
		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(Temp2Latest);
		
/*************************************************************************************/		
// 单次测试
//		JobNode jobNode = JobNodeModel.getJonNode4MergeXXXXObjectMergeDateDynamicValue("wf_qk_rel.MergeXXXXObject2Temp", 
//				rawXXXXObjectDir, 
//				"/RawData/wanfang/qk/rel/latest_20190507", 
//				"/RawData/wanfang/qk/rel/latest_outtest", 
//				200);
//		result.add(jobNode);
		
/*************************************************************************************/		
// 清理 latest
//		JobNode Temp2Latest4Clean = new JobNode("", "", 0,
//				"simple.jobstream.mapreduce.user.walker.wf_qk_rel.Temp2Latest4Clean");
//
//		result.add(Temp2Latest4Clean);

		return result;
	}
}
