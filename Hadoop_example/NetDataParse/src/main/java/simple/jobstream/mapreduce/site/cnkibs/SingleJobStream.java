package simple.jobstream.mapreduce.site.cnkibs;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";

		String rawDataDir = "/RawData/cnki/bs/big_json/20190706"; // 带解析新数据路径
		String rawDataXXXXObjectDir = "/RawData/cnki/bs/XXXXObject"; // 新数据解析后的XXXXObject格式数据路径
		String latest_tempDir = "/RawData/cnki/bs/latest_temp"; // 新数据和旧数据合并去重得到临时数据的存放路径
		String latestDir = "/RawData/cnki/bs/latest"; // 将latest_tempDir数据保存到该目录，以备下次更新使用 已经准换成A层
//		 String latestDir = "/RawData/cnki/bs/latest_bak_20190520";
		String jss_CDFDDir = "/RawData/cnki/bs/JSSXXXXObject/CDFD";
		String jss_CMFDDir = "/RawData/cnki/bs/JSSXXXXObject/CMFD";
		String new_data_xxxxobject = "/RawData/cnki/bs/new_data/xxxxobject"; // 去重后得到的新数据
		String new_data_stdDirA = "/RawData/cnki/bs/new_data/StdA"; // 新数据转换为DB3格式存放目录
		String new_data_stdDir2 = "/RawData/cnki/bs/new_data/StdBSZLF"; // 新数据转换为DB3格式存放目录
		String new_data_zhitu = "/RawData/cnki/bs/new_data/StdZhiTu"; // 生成智图的路径;
		String step1_html2xxxxobject_class = "simple.jobstream.mapreduce.site.cnkibs.Html2xxxxobject";
		String step2_merge_xxxxobject2temp_class = "simple.jobstream.mapreduce.site.cnkibs.MergeXXXXObject2Temp";
		String step3_gennewdata_class = "simple.jobstream.mapreduce.site.cnkibs.GenNewData";
		String step4_stdxxxxobject_class = "simple.jobstream.mapreduce.site.cnkibs.StdXXXXObject";
		String step_stdxxxxobject_class = "simple.jobstream.mapreduce.site.cnkibs.StdXXXXObject2ZLF";
		String step5_temp2latest_class = "simple.jobstream.mapreduce.site.cnkibs.Temp2Latest";
		// 第一步：解析json,获得数据
		JobNode Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("cnkibs.Json2XXXXObject",
				DateTimeHelper.getNowTimeAsBatch(), "simple.jobstream.mapreduce.site.cnkibs.Html2xxxxobjectA",
				rawDataDir, rawDataXXXXObjectDir, 20);

		// 第二步：将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp = JobNodeModel.getJonNode4MergeXXXXObject("cnkibs.MergeXXXXObject2Temp",
				rawDataXXXXObjectDir, latestDir, latest_tempDir, 200);

		// 第三步：生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("cnkibs.GenNewData", rawDataXXXXObjectDir,
				latest_tempDir, new_data_xxxxobject, 200);
		
		// 将A层搞成智图 /user/qianjun/cnki_bs/zhitu /user/qianjun/cnki_bs/zhilifang
		String AtoZhiTu = "simple.jobstream.mapreduce.site.cnkibs.StdXXXXObject2ForZhiTu";
		JobNode StdXXXXObjectAtoZhiTu = new JobNode("cnkibs.Std2Db3ZT", defaultRootDir, 0, AtoZhiTu);
		StdXXXXObjectAtoZhiTu.setConfig("inputHdfsPath", new_data_xxxxobject);
		StdXXXXObjectAtoZhiTu.setConfig("outputHdfsPath", new_data_zhitu);
//		result.add(StdXXXXObjectAtoZhiTu);
		
		// 第四步：生成A层
		JobNode Std2Db3A = JobNodeModel.getJobNode4Std2Db3("cnkibs.Std2Db3A",
				"simple.jobstream.mapreduce.common.vip.Std2Db3A", new_data_xxxxobject, new_data_stdDirA,
				"base_obj_meta_a_qk.cnkibs", "/RawData/_rel_file/base_obj_meta_a_template_dt.db3", 1);

		// 第五步：备份累积数据
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("cnkibs.Temp2Latest", latest_tempDir, latestDir);
//
//		result.add(Json2XXXXObject);
//		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
//		MergeXXXXObject2Temp.addChildJob(GenNewData);
//		GenNewData.addChildJob(Std2Db3A);
//		Std2Db3A.addChildJob(StdXXXXObjectAtoZhiTu);
//		StdXXXXObjectAtoZhiTu.addChildJob(Temp2Latest);

		// 两步走
//		result.add(Json2XXXXObject);
//		Json2XXXXObject.addChildJob(StdXXXXObjectAtoZhiTu);
		// 将latest中xxxx转成A层xxxxx
//		String latestDirA = "/RawData/cnki/bs/latestA";
//		String task = "simple.jobstream.mapreduce.site.cnkibs.ZhituXXXXObiectForA";
//		JobNode test_oldToA = new JobNode("oldChangA", defaultRootDir, 0,task);
//		test_oldToA.setConfig("inputHdfsPath", latestDir);
//		test_oldToA.setConfig("outputHdfsPath", latestDirA);
//		result.add(test_oldToA);
//		
		// 将A层xxx导出DB3查看/RawData/cnki/bs/test /user/qianjun/cnki_bs/baseDB
		// /user/qianjun/cnki_bs/output
//		JobNode Std2Db3Atest = JobNodeModel.getJobNode4Std2Db3("cnki_bs.Std2Db3A", "simple.jobstream.mapreduce.common.vip.Std2Db3A","/RawData/cnki/bs/test", 
//				  "/user/qianjun/cnki_bs/output", 
//		             "base_obj_meta_a_bs.cnki", 
//		             "/RawData/_rel_file/base_obj_meta_a_template_dt.db3", 
//		              1);
//		result.add(Std2Db3Atest);
//		  
		// 将A层搞成智图 /user/qianjun/cnki_bs/zhitu /user/qianjun/cnki_bs/zhilifang
//		String AtoZhiTu = "simple.jobstream.mapreduce.site.cnkibs.StdXXXXObject2ForZhiTu";
//		JobNode StdXXXXObjectAtoZhiTu = new JobNode("AxxxxObjectToZhiTu", defaultRootDir, 0, AtoZhiTu);
//		StdXXXXObjectAtoZhiTu.setConfig("inputHdfsPath", latestDir);
//		StdXXXXObjectAtoZhiTu.setConfig("outputHdfsPath", new_data_zhitu);
//		result.add(StdXXXXObjectAtoZhiTu);
//		  
		// 将A层导成智立方
//		String AtoZLF = "simple.jobstream.mapreduce.site.cnkibs.StdXXXXObject2ZLF2_new";
//		JobNode StdXXXXObjectAtoZLF = new JobNode("AxxxxObjectToZhiTu", defaultRootDir, 0, AtoZLF);
//		StdXXXXObjectAtoZLF.setConfig("inputHdfsPath", "/RawData/cnki/bs/latest");
//		StdXXXXObjectAtoZLF.setConfig("outputHdfsPath", "/user/ganruoxun/Temp_DB3/cnki_bs");
//		result.add(StdXXXXObjectAtoZLF);

		// 导出latest中全部数据
//		JobNode StdXXXXObjectToLatest = new JobNode("Step_4", defaultRootDir, 0, step4_stdxxxxobject_class);
//		StdXXXXObjectToLatest.setConfig("inputHdfsPath", latestDir);
//		StdXXXXObjectToLatest.setConfig("outputHdfsPath", new_data_zhitu);
//		result.add(StdXXXXObjectToLatest);

		// 添加任务结点
//		result.add(Html2XXXXObject);
////		Html2XXXXObject.addChildJob(Html2XXXXObject);
//	
//		Html2XXXXObject.addChildJob(MergeXXXXObject2Temp);
//		MergeXXXXObject2Temp.addChildJob(GenNewData);
//		GenNewData.addChildJob(StdXXXXObject);
//		StdXXXXObject.addChildJob(StdXXXXObject2zlf);
//		StdXXXXObject2zlf.addChildJob(Temp2Latest);
//		
		// 统计2019年到2010年，每年题录量
//		String step4_count_stdxxxxobject_class ="simple.jobstream.mapreduce.site.cnkibs.countYearCnkiBs";
//		JobNode countnum= new JobNode("Step_4", defaultRootDir, 0, step4_count_stdxxxxobject_class);
//		countnum.setConfig("inputHdfsPath", latestDir);
//		countnum.setConfig("outputHdfsPath", new_data_zhitu);
//		result.add(countnum);

		// 将A层xxx转换成智立方xxx
//		String latestDirZLF = "/RawData/cnki/bs/XXXXObject_ZLF";
//		String task = "simple.jobstream.mapreduce.site.cnkibs.AXXXXObiectForZlF";
//		JobNode test_AToZLF= new JobNode("AChangZLF", defaultRootDir, 0,task);
//		test_AToZLF.setConfig("inputHdfsPath", latestDir);
//		test_AToZLF.setConfig("outputHdfsPath", latestDirZLF);
//		result.add(test_AToZLF);

		// 将A层转换成智图
//		String latestDirZT = "/RawData/cnki/bs/latest_new";
//		String task = "simple.jobstream.mapreduce.site.cnkibs.AXXXXObiectForZhiTu2";
//		JobNode test_AToZT= new JobNode("AChangZT", defaultRootDir, 0,task);
//		test_AToZT.setConfig("inputHdfsPath", latestDir);
//		test_AToZT.setConfig("outputHdfsPath", latestDirZT);
//		result.add(test_AToZT);

		// 统计latest总量
//		String step4_count_xxxxobject_class ="simple.jobstream.mapreduce.site.cnkibs.countXXXXObjectLatest";
//		JobNode countnum= new JobNode("Step_4", defaultRootDir, 0, step4_count_xxxxobject_class);
//		countnum.setConfig("inputHdfsPath",latestDir);
//		countnum.setConfig("outputHdfsPath","/RawData/cnki/bs/test");
//		result.add(countnum);

//		JobNode StdXXXXObjecttest = new JobNode("Step_4", defaultRootDir, 0, step4_stdxxxxobject_class);
//		StdXXXXObjecttest.setConfig("inputHdfsPath", rawDataXXXXObjectDir);
//		StdXXXXObjecttest.setConfig("outputHdfsPath", new_data_zhitu);
//		// 直接生成DB3智图，解析，生成DB
//		result.add(Html2XXXXObject);
//		Html2XXXXObject.addChildJob(StdXXXXObjecttest);
		
		// 更新某一个字段 simple.jobstream.mapreduce.site.cnkibs
//		String task = "simple.jobstream.mapreduce.site.cnkibs.UpdateSubject";
//		JobNode test_AToZT= new JobNode("AChangZT", defaultRootDir, 0,task);
//		test_AToZT.setConfig("inputHdfsPath", latestDir);
//		test_AToZT.setConfig("outputHdfsPath", new_data_zhitu);
//		result.add(test_AToZT);
 
		 //根据全文id提取智立方数据		
		String AtoZLF = "simple.jobstream.mapreduce.site.cnkibs.StdXXXXObject2ZLF2_oldId";
		JobNode StdXXXXObjectAtoZLF = new JobNode("AxxxxObjectToZhiTu", defaultRootDir, 0, AtoZLF);
		StdXXXXObjectAtoZLF.setConfig("inputHdfsPath", latestDir);
		StdXXXXObjectAtoZLF.setConfig("outputHdfsPath", "/user/qianjun/cnki_bs/zhilifang");
		result.add(StdXXXXObjectAtoZLF);
		
		// 提出新旧ID
//		String ExportId= "simple.jobstream.mapreduce.site.cnkibs.ExoprtID";
//		JobNode ID= new JobNode("cnkibs_ID", defaultRootDir, 0, ExportId);
//		result.add(ID);
		

		return result;
	}
}
