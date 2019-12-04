package simple.jobstream.mapreduce.user.walker.cnki_qk;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String rootDir = "";

		String rawJsonDir = "/RawData/cnki/qk/detail/big_json/20190715";
		String rawXXXXObjectDir = "/RawData/cnki/qk/detail/XXXXObject";
		String latestTempDir = "/RawData/cnki/qk/detail/latest_temp"; // 临时成品目录
		String latestDir = "/RawData/cnki/qk/detail/latest"; // 成品目录
		String refLatestDir = "/RawData/cnki/qk/ref/latest";	// 引文的 latest
		String newXXXXObjectDir = "/RawData/cnki/qk/detail/new_data/XXXXObject"; // 本次新增的数据（XXXXObject）
		String std2Db3A = "/RawData/cnki/qk/detail/new_data/Std2Db3A";		// A层格式db3输出目录
		String std2Db3ZLF = "/RawData/cnki/qk/detail/new_data/Std2Db3ZLF";	// 智立方格式db3输出目录

		// 解析采集的原始数据
		JobNode Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("cnki_qk.Json2XXXXObject", 
				DateTimeHelper.getNowTimeAsBatch(),
				"simple.jobstream.mapreduce.user.walker.cnki_qk.Json2XXXXObject", 
				rawJsonDir, 
				rawXXXXObjectDir, 
				100);
		

		// 将历史累积数据和新数据合并去重（down_cnt 是日期类动态值）
		JobNode MergeXXXXObject2Temp = JobNodeModel.getJonNode4MergeXXXXObjectMergeDateDynamicValue(
				"cnki_qk.MergeXXXXObject2Temp",
				rawXXXXObjectDir, 
				latestDir, 
				latestTempDir, 
				400);
		
		// 合并引文ID、被引ID到主题录
		JobNode MergeRefidCitedid2Latest = JobNodeModel.getJobNode4MergeRefidCitedid("cnki_qk.MergeRefidCitedid2Latest", 
															refLatestDir, 
															"", 
															latestTempDir, 
															latestDir, 
															400);
		
		

		// 生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("cnki_qk.GenNewData", 
				rawXXXXObjectDir, latestDir, newXXXXObjectDir, 400);		

		// 以A层格式导出db3
		JobNode Std2Db3A = JobNodeModel.getJobNode4Std2Db3("cnki_qk.Std2Db3A", 
														"simple.jobstream.mapreduce.common.vip.Std2Db3A", 
														newXXXXObjectDir, 
														std2Db3A, 
														"base_obj_meta_a_qkwx.cnki_qk", 
														"/RawData/_rel_file/base_obj_meta_a_template_qkwx.db3", 
														50);
		
		
		// 以智立方格式导出到db3
		JobNode Std2Db3ZLF = JobNodeModel.getJobNode4Std2Db3("cnki_qk.Std2Db3ZLF",
												"simple.jobstream.mapreduce.user.walker.cnki_qk.Std2Db3ZLF", 
												newXXXXObjectDir, 
												std2Db3ZLF, 
												"cnki_qk.zlf", 
												"/RawData/cnki/qk/template/zlf_cnki_qk_template.db3", 
												30);

		// 统计每期文章量到 txt
		JobNode CountArticleGroupByBookid = new JobNode("CNKIQKParse", rootDir, 0,
				"simple.jobstream.mapreduce.site.cnki_qk.CountArticleGroupByBookid");
		// CountArticleGroupByBookid.setConfig("inputHdfsPath", latestDir);
		CountArticleGroupByBookid.setConfig("inputHdfsPath", newXXXXObjectDir);
		CountArticleGroupByBookid.setConfig("outputHdfsPath", "/RawData/cnki/qk/detail/count/CountArticleGroupByBookid");
		CountArticleGroupByBookid.setConfig("reduceNum", "1");

		// 统计每期文章量到access
		JobNode CountArticleGroupByBookid2Access = new JobNode("CNKIQKParse", rootDir, 0,
				"simple.jobstream.mapreduce.user.walker.cnki_qk.CountArticleGroupByBookid2Access");
		CountArticleGroupByBookid2Access.setConfig("export2access.inputTablePath", latestDir);
		CountArticleGroupByBookid2Access.setConfig("export2access.outputTablePath", "/RawData/cnki/qk/detail/count/CountArticleGroupByBookid2Access");
		CountArticleGroupByBookid2Access.setConfig("export2access.rednum", "1");
		String sql = "CREATE TABLE ArticleCount(pykm text(100), years text(100), num text(100), bookid text(100), cnt int)";
		CountArticleGroupByBookid2Access.setConfig("export2access.sSqlCreate", sql);

		// 正常更新
		result.add(Json2XXXXObject);
		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(MergeRefidCitedid2Latest);		// 合并引文ID、被引ID到主题录
		MergeRefidCitedid2Latest.addChildJob(CountArticleGroupByBookid2Access);		// 约50分钟
		MergeRefidCitedid2Latest.addChildJob(GenNewData);
		GenNewData.addChildJob(Std2Db3A);
		GenNewData.addChildJob(Std2Db3ZLF);

/*************************************************************************************/		
// 过滤导出		
		// 以智立方格式导出到db3
//		JobNode Std2Db3ZLFFilter = JobNodeModel.getJobNode4Std2Db3("cnki_qk.Std2Db3ZLFFilter",
//												"simple.jobstream.mapreduce.user.walker.cnki_qk.Std2Db3ZLFFilter", 
//												//"/RawData/cnki/qk/detail/new_data/XXXXObject", 
//												latestDir,
//												"/RawData/cnki/qk/detail/new_data/Std2Db3ZLFFilter", 
//												"cnki_qk.zlf", 
//												"/RawData/cnki/qk/template/zlf_cnki_qk_template.db3", 
//												1);
//		result.add(Std2Db3ZLFFilter);

/*************************************************************************************/		
//		JobNode Old2A = new JobNode("CNKIQKParse", "", 0,
//				"simple.jobstream.mapreduce.user.walker.cnki_qk.Old2A");
//		Old2A.setConfig("inputHdfsPath", "/RawData/cnki/qk/detail/latest_bak_20190223");	
//		Old2A.setConfig("outputHdfsPath", "/RawData/cnki/qk/detail/latest");	
//		result.add(Old2A);
		
/*************************************************************************************/
//		Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("cnki_qk.Json2XXXXObject", 
//				DateTimeHelper.getNowTimeAsBatch(),
//				"simple.jobstream.mapreduce.user.walker.cnki_qk.Json2XXXXObject", 
//				"/RawData/cnki/qk/detail/big_json/20190305/20190227_192.168.30.186_184_001_5964.big_json", 
////				rawJsonDir,
//				rawXXXXObjectDir, 
//				100);
//		Std2Db3A = JobNodeModel.getJobNode4Std2Db3A4QK("cnki_qk.Std2Db3A", 
//				rawXXXXObjectDir, 
//				std2Db3A, 
//				"base_obj_meta_a_qk.cnki_qk", 
//				"/RawData/_rel_file/base_obj_meta_a_template_qk.db3", 
//				1);
//		Std2Db3ZLF = JobNodeModel.getJobNode4Std2Db3("cnki_qk.Std2Db3ZLF",
//				"simple.jobstream.mapreduce.user.walker.cnki_qk.Std2Db3ZLF", 
//				rawXXXXObjectDir, 
//				std2Db3ZLF, 
//				"cnki_qk.zlf", 
//				"/RawData/cnki/qk/template/zlf_cnki_qk_template.db3", 
//				1);
//		result.add(Json2XXXXObject);
//		Json2XXXXObject.addChildJob(Std2Db3ZLF);
/*************************************************************************************/
// 以智立方格式导出所有
//		Std2Db3ZLF = JobNodeModel.getJobNode4Std2Db3("cnki_qk.Std2Db3ZLF",
//				"simple.jobstream.mapreduce.user.walker.cnki_qk.Std2Db3ZLF", 
//				latestDir, 
//				std2Db3ZLF, 
//				"cnki_qk.zlf", 
//				"/RawData/cnki/qk/template/zlf_cnki_qk_template.db3", 
//				300);
//		result.add(Std2Db3ZLF);
/*************************************************************************************/
// 以A层格式导出所有
//		Std2Db3A = JobNodeModel.getJobNode4Std2Db3A4QK("cnki_qk.Std2Db3A", 
//				latestDir, 
//				std2Db3A, 
//				"base_obj_meta_a_qk.cnki", 
//				"/RawData/_rel_file/base_obj_meta_a_template_qk.db3", 
//				200);
//		
//		result.add(Std2Db3A);		
/*************************************************************************************/
// 清理 latest
//		JobNode Temp2Latest4Clean = new JobNode("WFQKParse", "", 0,
//				"simple.jobstream.mapreduce.user.walker.cnki_qk.Temp2Latest4Clean");
//		result.add(Temp2Latest4Clean);

		return result;
	}

}
