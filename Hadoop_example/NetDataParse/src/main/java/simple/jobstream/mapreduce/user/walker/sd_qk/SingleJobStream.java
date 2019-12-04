package simple.jobstream.mapreduce.user.walker.sd_qk;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		
		String rawJsonDir = "/RawData/elsevier/sd_qk/big_json/20190716";
		String rawXXXXObjectDir = "/RawData/elsevier/sd_qk/XXXXObject";
		String latestTempDir = "/RawData/elsevier/sd_qk/latest_temp"; // 临时成品目录
		String latestDir = "/RawData/elsevier/sd_qk/latest"; // 成品目录
		String newXXXXObjectDir = "/RawData/elsevier/sd_qk/new_data/XXXXObject"; // 本次新增的数据（XXXXObject）
		String std2Db3A = "/RawData/elsevier/sd_qk/new_data/Std2Db3A";		// A层格式db3输出目录
		String std2Db3ZT = "/RawData/elsevier/sd_qk/new_data/Std2Db3ZT";	// 智立方格式db3输出目录
		
		// 解析采集的原始数据
		JobNode Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("sd_qk.Json2XXXXObject", 
				DateTimeHelper.getNowTimeAsBatch(),
				"simple.jobstream.mapreduce.user.walker.sd_qk.Json2XXXXObject", 
				rawJsonDir, 
				rawXXXXObjectDir, 
				50);
		
		// 将历史累积数据和新数据合并去重	
		JobNode MergeXXXXObject2Temp = JobNodeModel.getJonNode4MergeXXXXObject("sd_qk.MergeXXXXObject2Temp",
				rawXXXXObjectDir, 
				latestDir, 
				latestTempDir, 
				100);
		
		// 生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("sd_qk.GenNewData", 
						rawXXXXObjectDir, 
						latestTempDir, 
						newXXXXObjectDir, 
						100);	
		
		// 以A层格式导出db3
		JobNode Std2Db3A = JobNodeModel.getJobNode4Std2Db3("sd_qk.Std2Db3A", 
														"simple.jobstream.mapreduce.common.vip.Std2Db3A", 
														newXXXXObjectDir, 
														std2Db3A, 
														"base_obj_meta_a_qkwx.sd_qk", 
														"/RawData/_rel_file/base_obj_meta_a_template_qkwx.db3", 
														1);
		
		// 以智图格式导出到db3
		JobNode Std2Db3ZT = JobNodeModel.getJobNode4Std2Db3("sd_qk.Std2Db3ZT",
												"simple.jobstream.mapreduce.user.walker.sd_qk.Std2Db3ZT", 
												newXXXXObjectDir, 
												std2Db3ZT, 
												"sd_qk.zt", 
												"/RawData/_rel_file/zt_template.db3", 
												1);
		
		// 备份累积数据
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("sd_qk.Temp2Latest", latestTempDir, latestDir);
		
		// 正常更新
		result.add(Json2XXXXObject);
		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(Std2Db3A);
		GenNewData.addChildJob(Std2Db3ZT);
		GenNewData.addChildJob(Temp2Latest);

/*************************************************************************************/	
		// 以智图格式过滤导出
//		JobNode Std2Db3ZTFilter = JobNodeModel.getJobNode4Std2Db3("sd_qk.Std2Db3ZTFilter",
//				"simple.jobstream.mapreduce.user.walker.sd_qk.Std2Db3ZTFilter", 
//				latestDir, 
//				"/RawData/elsevier/sd_qk/new_data/Std2Db3ZTFilter", 
//				"sd_qk.zt",
//				"/RawData/_rel_file/zt_template.db3", 
//				1);
//		result.add(Std2Db3ZTFilter);

/*************************************************************************************/		
//		JobNode Old2A = new JobNode("", "", 0,
//				"simple.jobstream.mapreduce.user.walker.sd_qk.Old2A");
//		Old2A.setConfig("inputHdfsPath", "/RawData/elsevier/sd_qk/latest_bak20190529");	
//		Old2A.setConfig("outputHdfsPath", "/RawData/elsevier/sd_qk/latest");	
		
		
		// 以A层格式导出db3
//		JobNode Std2Db3A = JobNodeModel.getJobNode4Std2Db3("sd_qk.Std2Db3A", 
//														"simple.jobstream.mapreduce.common.vip.Std2Db3A", 
//														latestDir, 
//														std2Db3A, 
//														"base_obj_meta_a_qkwx.sd_qk", 
//														"/RawData/_rel_file/base_obj_meta_a_template_qkwx.db3", 
//														50);
		
		
//		result.add(Old2A);
//		Old2A.addChildJob(Std2Db3A);
		
/*************************************************************************************/	
// 单次解析测试
		// 解析采集的原始数据
//		Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("sd_qk.Json2XXXXObject",
//				DateTimeHelper.getNowTimeAsBatch(), 
//				"simple.jobstream.mapreduce.user.walker.sd_qk.Json2XXXXObject",
//				rawJsonDir, 
////				"/RawData/elsevier/sd_qk/test",
//				rawXXXXObjectDir, 
//				100);
//
//		// 以A层格式导出db3
//		Std2Db3A = JobNodeModel.getJobNode4Std2Db3("sd_qk.Std2Db3A", 
//				"simple.jobstream.mapreduce.common.vip.Std2Db3A",
//				rawXXXXObjectDir, 
//				std2Db3A, 
//				"base_obj_meta_a_qkwx.sd_qk",
//				"/RawData/_rel_file/base_obj_meta_a_template_qkwx.db3", 
//				10);
//
//		// 以智图格式导出到db3
//		Std2Db3ZT = JobNodeModel.getJobNode4Std2Db3("sd_qk.Std2Db3ZT",
//				"simple.jobstream.mapreduce.user.walker.sd_qk.Std2Db3ZT", 
//				rawXXXXObjectDir, 
//				std2Db3ZT, 
//				"sd_qk.zt",
//				"/RawData/_rel_file/zt_template.db3", 
//				10);
//		
//		result.add(Json2XXXXObject);
//		Json2XXXXObject.addChildJob(Std2Db3A);
//		Json2XXXXObject.addChildJob(Std2Db3ZT);
		
//		result.add(Std2Db3A);
//		result.add(Std2Db3ZT);		
		
/*************************************************************************************/
		// 以A层格式导出所有
//		Std2Db3A = JobNodeModel.getJobNode4Std2Db3("sd_qk.Std2Db3A.all", 
//				"simple.jobstream.mapreduce.common.vip.Std2Db3A",
//				latestDir, 
//				std2Db3A, 
//				"base_obj_meta_a_qkwx.sd_qk",
//				"/RawData/_rel_file/base_obj_meta_a_template_qkwx.db3", 
//				50);
//		result.add(Std2Db3A);
/*************************************************************************************/
// 以智图格式导出所有
//		Std2Db3ZT = JobNodeModel.getJobNode4Std2Db3("sd_qk.Std2Db3ZT.all",
//				"simple.jobstream.mapreduce.user.walker.sd_qk.Std2Db3ZT", 
//				latestDir, 
//				std2Db3ZT, 
//				"sd_qk.zt",
//				"/RawData/_rel_file/zt_template.db3", 
//				50);
//		result.add(Std2Db3ZT);
/*************************************************************************************/
// 导出ID
//		JobNode ExoprtID= new JobNode("sd_qk_id", "", 0, "simple.jobstream.mapreduce.user.walker.sd_qk.ExoprtID");
//		result.add(ExoprtID);
		

		return result;
	}
}
