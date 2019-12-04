package simple.jobstream.mapreduce.site.cnki_qk;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String rootDir = "";

		String rawHtmlDir = "/RawData/cnki/qk/detail/big_json/20190124";
		String rawXXXXObjectDir = "/RawData/cnki/qk/detail/XXXXObject";
		String latestTempDir = "/RawData/cnki/qk/detail/latest_temp"; // 临时成品目录
		String latestDir = "/RawData/cnki/qk/detail/latest"; // 成品目录
		String newXXXXObjectDir = "/RawData/cnki/qk/detail/new_data/XXXXObject"; // 本次新增的数据（XXXXObject）
		String stdDir = "/RawData/cnki/qk/detail/new_data/StdCNKIQK";

		// 解析采集的原始数据
//		JobNode Html2XXXXObject = new JobNode("CNKIQKParse", rootDir, 0,
//				"simple.jobstream.mapreduce.site.cnki_qk.Html2XXXXObject");
//		Html2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
//		Html2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);
		JobNode Html2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("cnki_qk.Html2XXXXObject", 
				DateTimeHelper.getNowTimeAsBatch(),
				"simple.jobstream.mapreduce.site.cnki_qk.Html2XXXXObject", 
				rawHtmlDir, 
				rawXXXXObjectDir, 
				10);
		

		// 将历史累积数据和新数据合并去重
//		JobNode MergeXXXXObject2Temp = new JobNode("CNKIQKParse", rootDir, 0,
//				"simple.jobstream.mapreduce.site.cnki_qk.MergeXXXXObject2Temp");
//		MergeXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
//		MergeXXXXObject2Temp.setConfig("outputHdfsPath", latest_tempDir);

		JobNode MergeXXXXObject2Temp = JobNodeModel.getJonNode4MergeXXXXObject("cnki_qk.MergeXXXXObject2Temp",
				rawXXXXObjectDir, latestDir, latestTempDir, 200);

		// 生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
//		JobNode GenNewData = new JobNode("CNKIQKParse", rootDir, 0,
//				"simple.jobstream.mapreduce.site.cnki_qk.GenNewData");
//		GenNewData.setConfig("inputHdfsPath", latest_tempDir + "," + rawXXXXObjectDir);
//		GenNewData.setConfig("outputHdfsPath", newXXXXObjectDir);
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("cnki_qk.GenNewData", 
				rawXXXXObjectDir, latestTempDir, newXXXXObjectDir, 200);		

		// 导出数据到 db3
//		JobNode StdCNKIQK = new JobNode("CNKIQKParse", rootDir, 0, "simple.jobstream.mapreduce.site.cnki_qk.StdCNKIQK");
//		StdCNKIQK.setConfig("inputHdfsPath", newXXXXObjectDir);
//		StdCNKIQK.setConfig("outputHdfsPath", stdDir);
//		StdCNKIQK.setConfig("reduceNum", "1");
		JobNode StdCNKIQK = JobNodeModel.getJobNode4Std2Db3("cnki_qk.Std2Db3",
				"simple.jobstream.mapreduce.site.cnki_qk.Std2Db3", 
				newXXXXObjectDir, 
				stdDir, 
				"cnki_qk", 
				"/RawData/cnki/qk/template/cnki_qk_template.db3", 
				1);

		// 备份累积数据
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("cnki_qk.Temp2Latest", latestTempDir, latestDir);

		// 统计单期文章量
		JobNode CountArticleGroupByBookid = new JobNode("CNKIQKParse", rootDir, 0,
				"simple.jobstream.mapreduce.site.cnki_qk.CountArticleGroupByBookid");
		// CountArticleGroupByBookid.setConfig("inputHdfsPath", latestDir);
		CountArticleGroupByBookid.setConfig("inputHdfsPath", newXXXXObjectDir);
		CountArticleGroupByBookid.setConfig("outputHdfsPath", "/RawData/cnki/qk/detail/count/CountArticleGroupByBookid");
		CountArticleGroupByBookid.setConfig("reduceNum", "1");

		// 统计单文章量到access
		JobNode CountArticleGroupByBookid2Access = new JobNode("CNKIQKParse", rootDir, 0,
				"simple.jobstream.mapreduce.site.cnki_qk.CountArticleGroupByBookid2Access");
		CountArticleGroupByBookid2Access.setConfig("export2access.inputTablePath", latestDir);
		CountArticleGroupByBookid2Access.setConfig("export2access.outputTablePath", "/RawData/cnki/qk/detail/count/CountArticleGroupByBookid2Access");
		CountArticleGroupByBookid2Access.setConfig("export2access.rednum", "1");
		String sql = "CREATE TABLE ArticleCount(pykm text(100), years text(100), num text(100), bookid text(100), cnt int)";
		CountArticleGroupByBookid2Access.setConfig("export2access.sSqlCreate", sql);

		// 正常更新
//		result.add(Html2XXXXObject);
//		Html2XXXXObject.addChildJob(MergeXXXXObject2Temp);
//		MergeXXXXObject2Temp.addChildJob(GenNewData);
//		GenNewData.addChildJob(StdCNKIQK);
//		StdCNKIQK.addChildJob(Temp2Latest);
//		Temp2Latest.addChildJob(CountArticleGroupByBookid2Access);

		// 单次测试
		JobNode Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("cnki_qk.Json2XXXXObject", 
																		DateTimeHelper.getNowTimeAsBatch(),
																		"simple.jobstream.mapreduce.site.cnki_qk.Json2XXXXObject", 
																		rawHtmlDir, 
																		rawXXXXObjectDir, 
																		2);
//		JobNode Std2Db3A = JobNodeModel.getJobNode4Std2Db3A4QK("cnki_qk.Std2Db3A", 
//															rawXXXXObjectDir, 
//															stdDir, 
//															"base_obj_meta_a_qk.cnki_qk", 
//															"/RawData/_rel_file/base_obj_meta_a_template_qk.db3", 
//															1);
		result.add(Json2XXXXObject);
//		Json2XXXXObject.addChildJob(Std2Db3A);

		return result;
	}

}
