package simple.jobstream.mapreduce.user.walker.wf_qk_ref;

import java.util.LinkedHashSet;

import org.apache.avro.LogicalTypes.Date;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

// 万方引文
public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();

		String rawJsonDir = "/RawData/wanfang/qk/ref/big_json/20190709";
		String rawXXXXObjectDir = "/RawData/wanfang/qk/ref/XXXXObject";
		String latestTempDir = "/RawData/wanfang/qk/ref/latest_temp"; // 临时成品目录
		String latestDir = "/RawData/wanfang/qk/ref/latest"; // 成品目录
		String latestExportDir = "/RawData/wanfang/qk/ref/latest_export"; // 成品db3目录
		String newXXXXObjectDir = "/RawData/wanfang/qk/ref/new_data/XXXXObject"; // 本次新增的数据（XXXXObject）
		String std2Db3A = "/RawData/wanfang/qk/ref/new_data/Std2Db3A";
		String std2Db3ZLF = "/RawData/wanfang/qk/ref/new_data/Std2Db3ZLF";

		// 解析 json 文件到 XXXXObject
		JobNode Json2XXXXObject = JobNodeModel.getJobNode4ParseRef2XXXXObject("wf_qk_ref.Json2XXXXObject", 
				DateTimeHelper.getNowTimeAsBatch(), 
				"simple.jobstream.mapreduce.user.walker.wf_qk_ref.Json2XXXXObject", 
				rawJsonDir, 
				rawXXXXObjectDir, 
				100);

		// 将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp = JobNodeModel.getJonNode4MergeXXXXObject4Ref("wf_qk_ref.MergeXXXXObject2Temp",
														rawXXXXObjectDir, 
														latestDir, 
														latestTempDir, 
														400);

		// 生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
//		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject4Ref("wf_qk_ref.GenNewData", 
//																		rawXXXXObjectDir, 
//																		latestTempDir, 
//																		newXXXXObjectDir, 
//																		400);

		
		// 以A层格式导出到db3
		JobNode Std2Db3A4Ref = JobNodeModel.getJobNode4Std2Db3("wf_qk_ref.Std2Db3A", 
															"simple.jobstream.mapreduce.common.vip.Std2Db3A",  
															rawXXXXObjectDir, 
															std2Db3A, 
															"base_obj_ref_a.wf_qk",
															"/RawData/_rel_file/base_obj_ref_a_template.db3", 
															10);
				
		
		// 以智立方格式导出到db3
		JobNode Std2Db3ZLF4Ref = JobNodeModel.getJobNode4Std2Db3("wf_qk_ref.Std2Db3ZLF",
												"simple.jobstream.mapreduce.user.walker.wf_qk_ref.Std2Db3ZLF4Ref", 
												rawXXXXObjectDir, 
												std2Db3ZLF, 
												"wf_qk_ref.zlf", 
												"/RawData/wanfang/qk/template/zlf_wanfang_qk_ref_template.db3", 
												1);

		// 备份累积数据
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("wf_qk_ref.Temp2Latest", 
													latestTempDir, 
													latestDir);

		// 正常更新
		result.add(Json2XXXXObject);
		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		Json2XXXXObject.addChildJob(Std2Db3A4Ref);
		Json2XXXXObject.addChildJob(Std2Db3ZLF4Ref);
		MergeXXXXObject2Temp.addChildJob(Temp2Latest);
		
/*************************************************************************************/		
// 导出所有数据
//		Std2Db3ZLF4Ref = JobNodeModel.getJobNode4Std2Db3("wf_qk_ref.Std2Db3ZLF",
//				"simple.jobstream.mapreduce.user.walker.wf_qk_ref.Std2Db3ZLF4Ref", 
//				latestDir, 
//				std2Db3ZLF, 
//				"wf_qk_ref.zlf", 
//				"/RawData/wanfang/qk/template/zlf_wanfang_qk_ref_template.db3", 
//				400);
//		result.add(Std2Db3ZLF4Ref);
		
/*************************************************************************************/		
// 单次测试
//		result.add(Json2XXXXObject);
//		Json2XXXXObject.addChildJob(Std2Db3ZLF4Ref);		

/*************************************************************************************/		
//		JobNode Old2A = JobNodeModel.getJobNode4ParseRef2XXXXObject("wf_qk_ref.Old2A", 
//										DateTimeHelper.getNowTimeAsBatch(), 
//										"simple.jobstream.mapreduce.user.walker.wf_qk_ref.Old2A", 
//										"/user/ganruoxun/WFBasicInfo/UpdateTitle/TitleInfo", 
//										"/RawData/wanfang/qk/ref/latest", 
//										300);
//		result.add(Old2A);

		return result;
	}
}
