package simple.jobstream.mapreduce.site.wf_qk;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";

		String rawHtmlDir = "/RawData/wanfang/qk/detail/big_json/20190122";
		String rawXXXXObjectDir = "/RawData/wanfang/qk/detail/XXXXObject";
		String latestTempDir = "/RawData/wanfang/qk/detail/latest_temp"; // 临时成品目录
		String latestDir = "/RawData/wanfang/qk/detail/latest"; // 成品目录
		String citedLatestDir = "/RawData/wanfang/qk/cited/latest"; // 引文量、被引量的 latest
		String newXXXXObjectDir = "/RawData/wanfang/qk/detail/new_data/XXXXObject"; // 本次新增的数据（XXXXObject）
		String stdDir = "/RawData/wanfang/qk/detail/new_data/StdWFQK";

//		JobNode Json2XXXXObject = new JobNode("WFQKParse", defaultRootDir, 0,
//				"simple.jobstream.mapreduce.site.wf_qk.Json2XXXXObject");
//		Json2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
//		Json2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);
		JobNode Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("wf_qk.Json2XXXXObject", 
				DateTimeHelper.getNowTimeAsBatch(),
				"simple.jobstream.mapreduce.site.wf_qk.Json2XXXXObject", 
				rawHtmlDir, 
				rawXXXXObjectDir, 
				10);

		// 将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp = new JobNode("WFQKParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.wf_qk.MergeXXXXObject2Temp");
		MergeXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
		MergeXXXXObject2Temp.setConfig("outputHdfsPath", latestTempDir);

		// 合并引文量/被引量到主题录
		JobNode MergeCited2Latest = new JobNode("WFQKParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.wf_qk.MergeCited2Latest");
		MergeCited2Latest.setConfig("inputHdfsPath", latestTempDir + "," + citedLatestDir);
		MergeCited2Latest.setConfig("outputHdfsPath", latestDir);

		// 生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = new JobNode("WFQKParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.wf_qk.GenNewData");
		GenNewData.setConfig("inputHdfsPath", latestDir + "," + rawXXXXObjectDir);
		GenNewData.setConfig("outputHdfsPath", newXXXXObjectDir);

		JobNode Std2Db3 = new JobNode("WFQKParse", defaultRootDir, 0, "simple.jobstream.mapreduce.site.wf_qk.Std2Db3");
		Std2Db3.setConfig("inputHdfsPath", newXXXXObjectDir);
		Std2Db3.setConfig("outputHdfsPath", stdDir);
		Std2Db3.setConfig("reduceNum", "1");

		// 统计单文章量到access
		JobNode CountArticleGroupByBookid2Access = new JobNode("WFQKParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.wf_qk.CountArticleGroupByBookid2Access");
		CountArticleGroupByBookid2Access.setConfig("export2access.inputTablePath", latestDir);
		CountArticleGroupByBookid2Access.setConfig("export2access.outputTablePath",
				"/RawData/wanfang/qk/detail/count/CountArticleGroupByBookid2Access");
		CountArticleGroupByBookid2Access.setConfig("export2access.rednum", "1");
		String sql = "CREATE TABLE ArticleCount(pykm text(100), years text(100), num text(100), bookid text(100), cnt int)";
		CountArticleGroupByBookid2Access.setConfig("export2access.sSqlCreate", sql);

		// 导出过滤数据到 db3
		JobNode Std2Db3Filter = JobNodeModel.getJobNode4Std2Db3("wf_qk.Std2Db3Filter",
				"simple.jobstream.mapreduce.site.wf_qk.Std2Db3Filter", 
				latestDir, 
				stdDir, 
				"wf_qk",
				"/RawData/wanfang/qk/template/wanfang_qk_template.db3", 
				1);

		// 正常更新
//		result.add(Json2XXXXObject);
//		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
//		MergeXXXXObject2Temp.addChildJob(MergeCited2Latest);
//		MergeCited2Latest.addChildJob(GenNewData);
//		GenNewData.addChildJob(Std2Db3);
//		Std2Db3.addChildJob(CountArticleGroupByBookid2Access);

		// 单次测试
		result.add(MergeCited2Latest);
		MergeCited2Latest.addChildJob(GenNewData);
		GenNewData.addChildJob(Std2Db3);

		// 万方医学
		JobNode Std2Db3Med = new JobNode("WFQKParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.wf_qk.Std2Db3Med");
		Std2Db3Med.setConfig("inputHdfsPath", newXXXXObjectDir);
		Std2Db3Med.setConfig("outputHdfsPath", "/RawData/wanfang/qk/detail/med/new_data");
		Std2Db3Med.setConfig("reduceNum", "1");
		
		JobNode CountMed = new JobNode("WFQKParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.wf_qk.CountMed");
		CountMed.setConfig("inputHdfsPath", "/RawData/wanfang/qk/detail/latest");	
		CountMed.setConfig("outputHdfsPath", "/RawData/wanfang/qk/detail/med/CountMed");

//		result.add(Std2Db3Med);
//		CountArticleGroupByBookid2Access.addChildJob(Std2Db3Med);
//		Std2Db3Med.addChildJob(CountMed);

		return result;
	}
}
