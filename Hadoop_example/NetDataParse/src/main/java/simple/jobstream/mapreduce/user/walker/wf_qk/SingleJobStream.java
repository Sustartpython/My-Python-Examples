package simple.jobstream.mapreduce.user.walker.wf_qk;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

// 万方期刊
public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";

//		String rawHtmlDir = "/RawData/wanfang/qk/detail/big_json/20190211/20190130_192.168.30.188_3000_001.big_json";
		String rawJsonDir = "/RawData/wanfang/qk/detail/big_json/20190709";		
		
		String rawXXXXObjectDir = "/RawData/wanfang/qk/detail/XXXXObject";
		String latestTempDir = "/RawData/wanfang/qk/detail/latest_temp"; // 临时成品目录
		String latestTempWithRelDir = "/RawData/wanfang/qk/detail/latest_temp_with_rel"; // 临时成品目录（包含引文量、被引量）
		String latestDir = "/RawData/wanfang/qk/detail/latest"; // 成品目录
		String relLatestDir = "/RawData/wanfang/qk/rel/latest"; // 引文量、被引量的 latest
		String refLatestDir = "/RawData/wanfang/qk/ref/latest"; // 引文的 latest
		String newXXXXObjectDir = "/RawData/wanfang/qk/detail/new_data/XXXXObject"; // 本次新增的数据（XXXXObject）
		String std2Db3A = "/RawData/wanfang/qk/detail/new_data/Std2Db3A"; // A层格式db3输出目录
		String std2Db3ZLF = "/RawData/wanfang/qk/detail/new_data/Std2Db3ZLF"; // 智立方格式db3输出目录

		JobNode Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("wf_qk.Json2XXXXObject",
				DateTimeHelper.getNowTimeAsBatch(), "simple.jobstream.mapreduce.user.walker.wf_qk.Json2XXXXObject",
				rawJsonDir, rawXXXXObjectDir, 200);

		// 将历史累积数据和新数据合并去重（down_cnt 是日期类动态值）
		JobNode MergeXXXXObject2Temp = JobNodeModel.getJonNode4MergeXXXXObjectMergeDateDynamicValue(
				"wf_qk.MergeXXXXObject2Temp", rawXXXXObjectDir, latestDir, latestTempDir, 400);

		// 合并引文量/被引量到主题录
		JobNode MergeRel2LatestWithRel = JobNodeModel.getJobNode4MergeRefcntCitedcnt("wf_qk.MergeRel2LatestWithRel",
				relLatestDir, relLatestDir, latestTempDir, latestTempWithRelDir, 400);

		// 合并引文ID、被引ID到主题录
		JobNode MergeRefidCitedid2Latest = JobNodeModel.getJobNode4MergeRefidCitedid("wf_qk.MergeRefidCitedid2Latest",
				refLatestDir, "", latestTempWithRelDir, latestDir, 400);

		// 生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("wf_qk.GenNewData", rawXXXXObjectDir, latestDir,
				newXXXXObjectDir, 100);

		// 以A层格式导出到db3
		JobNode Std2Db3A = JobNodeModel.getJobNode4Std2Db3("wf_qk.Std2Db3A",
				"simple.jobstream.mapreduce.common.vip.Std2Db3A", newXXXXObjectDir, std2Db3A, "base_obj_meta_a.wf_qk",
				"/RawData/_rel_file/base_obj_meta_a_template_qkwx.db3", 10);

		// 以智立方格式导出到db3
		JobNode Std2Db3ZLF = JobNodeModel.getJobNode4Std2Db3("wf_qk.Std2Db3ZLF",
				"simple.jobstream.mapreduce.user.walker.wf_qk.Std2Db3ZLF", newXXXXObjectDir, std2Db3ZLF, "wf_qk.zlf",
				"/RawData/wanfang/qk/template/zlf_wanfang_qk_template.db3", 2);

		// 统计单文章量到access
		JobNode CountArticleGroupByBookid2Access = new JobNode("WFQKParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.user.walker.wf_qk.CountArticleGroupByBookid2Access");
		CountArticleGroupByBookid2Access.setConfig("export2access.inputTablePath", latestDir);
		CountArticleGroupByBookid2Access.setConfig("export2access.outputTablePath",
				"/RawData/wanfang/qk/detail/count/CountArticleGroupByBookid2Access");
		CountArticleGroupByBookid2Access.setConfig("export2access.rednum", "1");
		String sql = "CREATE TABLE ArticleCount(pykm text(100), years text(100), num text(100), bookid text(100), cnt int)";
		CountArticleGroupByBookid2Access.setConfig("export2access.sSqlCreate", sql);

		// 正常更新
		result.add(Json2XXXXObject); // 解析原始数据
		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp); // 合并数据到 latest_temp
		MergeXXXXObject2Temp.addChildJob(MergeRel2LatestWithRel); // 合并引用、被引关系到 latest_temp_with_rel
		MergeRel2LatestWithRel.addChildJob(MergeRefidCitedid2Latest); // 合并引文ID、引证文献ID到 latest
		MergeRefidCitedid2Latest.addChildJob(CountArticleGroupByBookid2Access); // 统计期次到 access，约半个小时
		MergeRefidCitedid2Latest.addChildJob(GenNewData); // 生成新数据
		GenNewData.addChildJob(Std2Db3A); // 以A层格式导出数据到 db3
		GenNewData.addChildJob(Std2Db3ZLF); // 以智立方格式导出数据到 db3


///*************************************************************************************/
// 过滤导出
		// 以智立方格式导出到db3
//		JobNode Std2Db3ZLFFilter = JobNodeModel.getJobNode4Std2Db3("wf_qk.Std2Db3ZLFFilter",
//										"simple.jobstream.mapreduce.user.walker.wf_qk.Std2Db3ZLFFilter", 
//										latestDir, 
//										"/RawData/wanfang/qk/detail/new_data/Std2Db3ZLFFilter", 
//										"wf_qk.zlf",
//										"/RawData/wanfang/qk/template/zlf_wanfang_qk_template.db3", 
//										1);
//		result.add(Std2Db3ZLFFilter);

///*************************************************************************************/
// 导出所有数据
//		Std2Db3ZLF = JobNodeModel.getJobNode4Std2Db3("wf_qk.Std2Db3ZLF",
//				"simple.jobstream.mapreduce.user.walker.wf_qk.Std2Db3ZLF", 
//				latestDir, 
//				std2Db3ZLF, 
//				"wf_qk.zlf", 
//				"/RawData/wanfang/qk/template/zlf_wanfang_qk_template.db3", 
//				1);
//		result.add(Std2Db3ZLF);	

		/*************************************************************************************/
// 单次测试
//		Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("wf_qk.Json2XXXXObject",
//				DateTimeHelper.getNowTimeAsBatch(), 
//				"simple.jobstream.mapreduce.user.walker.wf_qk.Json2XXXXObject",
//				rawJsonDir, 
//				rawXXXXObjectDir, 
//				10);
//		
//		Std2Db3A = JobNodeModel.getJobNode4Std2Db3("wf_qk.Std2Db3A",
//									"simple.jobstream.mapreduce.common.vip.Std2Db3A", 
//									rawXXXXObjectDir, 
//									std2Db3A, 
//									"base_obj_meta_a.wf_qk",
//									"/RawData/_rel_file/base_obj_meta_a_template_qkwx.db3", 
//									2);
//		
//		Std2Db3ZLF = JobNodeModel.getJobNode4Std2Db3("wf_qk.Std2Db3ZLF",
//				"simple.jobstream.mapreduce.user.walker.wf_qk.Std2Db3ZLF", 
//				rawXXXXObjectDir, 
//				std2Db3ZLF, 
//				"wf_qk.zlf", 
//				"/RawData/wanfang/qk/template/zlf_wanfang_qk_template.db3", 
//				2);
//		
//		result.add(Json2XXXXObject);
//		Json2XXXXObject.addChildJob(Std2Db3A);
//		Json2XXXXObject.addChildJob(Std2Db3ZLF);

		/*************************************************************************************/
//		JobNode Old2A = new JobNode("WFQKParse", defaultRootDir, 0,
//				"simple.jobstream.mapreduce.user.walker.wf_qk.Old2A");
//		Old2A.setConfig("inputHdfsPath", "/RawData/wanfang/qk/detail/latest_bak_20190213");	
//		Old2A.setConfig("outputHdfsPath", "/RawData/wanfang/qk/detail/latest");	
//		result.add(Old2A);

		/*************************************************************************************/
//		Std2Db3ZLF = JobNodeModel.getJobNode4Std2Db3("wf_qk.Std2Db3ZLF",
//				"simple.jobstream.mapreduce.user.walker.wf_qk.Std2Db3ZLF", 
//				"/RawData/wanfang/qk/detail/XXXXObject/part-r-00000", 
//				"/RawData/wanfang/qk/detail/new_data/Std2Db3ZLF", 
//				"zlf.wf_qk", 
//				"/RawData/wanfang/qk/template/zlf_wanfang_qk_template.db3", 
//				1);
//		result.add(Std2Db3ZLF);

/*************************************************************************************/
// 清理 latest
//		JobNode Temp2Latest4Clean = new JobNode("WFQKParse", defaultRootDir, 0,
//				"simple.jobstream.mapreduce.user.walker.wf_qk.Temp2Latest4Clean");
//		result.add(Temp2Latest4Clean);		
//*************************************************************************************/
// 备份 latest
//		JobNode bakLatest = JobNodeModel.getJobNode4CopyXXXXObject("wf_qk.bak", 
//				latestDir, 
//				"/RawData/wanfang/qk/detail/latest_bak20190626");
//		result.add(bakLatest);

		return result;
	}
}
