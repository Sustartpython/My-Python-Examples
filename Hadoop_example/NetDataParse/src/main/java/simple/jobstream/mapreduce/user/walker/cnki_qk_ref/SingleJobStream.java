package simple.jobstream.mapreduce.user.walker.cnki_qk_ref;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();

		String rawJsonDir = "/RawData/cnki/qk/ref/big_json/20190715";
		String rawXXXXObjectDir = "/RawData/cnki/qk/ref/XXXXObject";
		String latestTempDir = "/RawData/cnki/qk/ref/latest_temp"; 	// 临时成品目录
		String latestDir = "/RawData/cnki/qk/ref/latest"; 				// 成品目录
		String latestExportDir = "/RawData/cnki/qk/ref/latest_export"; 	// 成品db3目录
		String std2Db3A = "/RawData/cnki/qk/ref/new_data/Std2Db3A";
		String std2Db3ZLF = "/RawData/cnki/qk/ref/new_data/Std2Db3ZLF";

		/**/
		JobNode Json2XXXXObject = JobNodeModel.getJobNode4ParseRef2XXXXObject("cnki_qk_ref.Json2XXXXObject", 
				DateTimeHelper.getNowTimeAsBatch(), 
				"simple.jobstream.mapreduce.user.walker.cnki_qk_ref.Json2XXXXObject", 
				rawJsonDir, 
				rawXXXXObjectDir, 
				100);

		// 将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp = JobNodeModel.getJonNode4MergeXXXXObject4Ref("cnki_qk_ref.MergeXXXXObject2Temp",
				rawXXXXObjectDir, 
				latestDir, 
				latestTempDir, 
				400);
		
		// 以A层格式导出到db3
		JobNode Std2Db3A = JobNodeModel.getJobNode4Std2Db3("cnki_qk_ref.Std2Db3A", 
													"simple.jobstream.mapreduce.common.vip.Std2Db3A", 
													rawXXXXObjectDir, 
													std2Db3A, 
													"base_obj_ref_a.cnki_qk", 
													"/RawData/_rel_file/base_obj_ref_a_template.db3", 
													30);
		
		// 以智立方格式导出到db3
		JobNode Std2Db3ZLF = JobNodeModel.getJobNode4Std2Db3("cnki_qk_ref.Std2Db3ZLF",
												"simple.jobstream.mapreduce.user.walker.cnki_qk_ref.Std2Db3ZLF4Ref", 
												rawXXXXObjectDir, 
												std2Db3ZLF, 
												"cnki_qk_ref.zlf", 
												"/RawData/cnki/qk/template/zlf_cnki_qk_ref_template.db3", 
												30);


		// 备份累积数据
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("cnki_qk_ref.Temp2Latest", 
				latestTempDir, 
				latestDir);

		// 正常更新
		result.add(Json2XXXXObject);
		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		Json2XXXXObject.addChildJob(Std2Db3A);
		Json2XXXXObject.addChildJob(Std2Db3ZLF);
		MergeXXXXObject2Temp.addChildJob(Temp2Latest);

/**************************************************************************************************/
//		JobNode Old2A = JobNodeModel.getJobNode4ParseRef2XXXXObject("cnki_qk_ref.Old2A", 
//				DateTimeHelper.getNowTimeAsBatch(), 
//				"simple.jobstream.mapreduce.user.walker.cnki_qk_ref.Old2A", 
//				"/user/ganruoxun/CNKIBasicInfo/TitleInfo/TitleInfo", 
//				"/RawData/cnki/qk/ref/latest", 
//				300);
//		result.add(Old2A);		
/**************************************************************************************************/
// 导出所有
//		Std2Db3ZLF = JobNodeModel.getJobNode4Std2Db3("cnki_qk_ref.Std2Db3ZLF",
//				"simple.jobstream.mapreduce.user.walker.cnki_qk_ref.Std2Db3ZLF4Ref", 
//				latestDir, 
//				std2Db3ZLF, 
//				"cnki_qk_ref.zlf", 
//				"/RawData/cnki/qk/template/zlf_cnki_qk_ref_template.db3", 
//				400);
//		result.add(Std2Db3ZLF);

		return result;
	}
}
