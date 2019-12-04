package simple.jobstream.mapreduce.site.cnki_hy;

import java.util.LinkedHashSet;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

import com.vipcloud.JobNode.JobNode;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		
		String rawHtmlDir = "/RawData/cnki/hy/big_htm/big_htm_20190711";//文件输入目录
		String rawXXXXObjectDir = "/RawData/cnki/hy/XXXXObject";//新XXXXOBj 输出目录
		String latest_tempDir = "/RawData/cnki/hy/latest_temp";	//新数据合并老数据后生成，新一轮的总数据存放地址
		String latestDir = "/RawData/cnki/hy/latest";	//成品目录
		String newXXXXObjectDir = "/RawData/cnki/hy/new_data/XXXXObject";//本次新增的数据（XXXXObject）
		String stdDirZT = "/RawData/cnki/hy/new_data/std_zhitu";//智图DB3目录
		String stdDirA = "/RawData/cnki/hy/new_data/std_A";//A层DB3目录
		String stdDirZLF = "/RawData/cnki/hy/new_data/std_zlf";
		
		// 第一步：将最近增加的big_htm文件转换为xxxxobjcet文件存放在xxxxobject目录下
		JobNode Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("cnki_hy.Json2XXXXObject",
		DateTimeHelper.getNowTimeAsBatch(), "simple.jobstream.mapreduce.site.cnki_hy.Json2XXXXObject",
		rawHtmlDir, rawXXXXObjectDir, 10);

		// 将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp = JobNodeModel.getJonNode4MergeXXXXObject("cnki_hy.MergeXXXXObject2Temp",
				rawXXXXObjectDir, latestDir, latest_tempDir, 200);

//		// 生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("cnki_hy.GenNewData", rawXXXXObjectDir,
				latest_tempDir, newXXXXObjectDir, 200);
//
		// 备份累积数据
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("cnki_hy.Temp2Latest", latest_tempDir,
				latestDir);
		
		// 导出智图
		JobNode StdXXXXObjectZhiTu = JobNodeModel.getJobNode4Std2Db3("cnki_hy.StdZhiTu",
				"simple.jobstream.mapreduce.site.cnki_hy.StdXXXXObjectForZhiTu",newXXXXObjectDir,stdDirZT,
				"cnki_hy", "/RawData/_rel_file/zt_template.db3", 1);	
//		result.add(StdXXXXObjectZhiTu);
		
		// 导出A层
		JobNode Std2Db3Atest = JobNodeModel.getJobNode4Std2Db3("cnki_hy.Std2Db3A", "simple.jobstream.mapreduce.common.vip.Std2Db3A",newXXXXObjectDir, 
		  stdDirA, 
         "base_obj_meta_a_bs.cnki_hy", 
         "/RawData/_rel_file/base_obj_meta_a_template_hy.db3", 
          1);
		
//		result.add(Json2XXXXObject);
//		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
//		MergeXXXXObject2Temp.addChildJob(GenNewData);
//		GenNewData.addChildJob( StdXXXXObjectZhiTu);
//		StdXXXXObjectZhiTu.addChildJob(Std2Db3Atest);
//		Std2Db3Atest.addChildJob(Temp2Latest);



		// 将latest中xxxx转成A层xxxxx
//		String latestDirA = "/RawData/cnki/hy/latestA";
//		String task = "simple.jobstream.mapreduce.site.cnki_hy.ZTXXXXObiectForA";
//		JobNode test_oldToA = new JobNode("oldChangA", defaultRootDir, 0,task);
//		test_oldToA.setConfig("inputHdfsPath", "/RawData/cnki/hy/latest");
//		test_oldToA.setConfig("outputHdfsPath", latestDirA);
//		
		// 将latest中xxxx直接导出A层db3
//		JobNode Std2Db3Atest = JobNodeModel.getJobNode4Std2Db3("cnki_hy.Std2Db3A", "simple.jobstream.mapreduce.common.vip.Std2Db3A","/RawData/cnki/hy/latestA", 
//		  "/RawData/cnki/hy/new_data/std_A", 
//           "base_obj_meta_a_bs.cnki_hy", 
//           "/RawData/_rel_file/base_obj_meta_a_template_hy.db3", 
//            10);
////		result.add(test_oldToA);
////		test_oldToA.addChildJob(Std2Db3Atest);
//		
//		// 将A层XXX转换成智图DB
//		String DBpath = "/RawData/cnki/hy/new_data/std_zhitu";	
//		JobNode StdXXXXObjectZhiTu = JobNodeModel.getJobNode4Std2Db3("cnki_hy.StdZhiTu",
//				"simple.jobstream.mapreduce.site.cnki_hy.StdXXXXObjectForZhiTu",latestDir, DBpath,
//				"cnki_hy", "/RawData/_rel_file/zt_template.db3", 5);	
//		
//		// 新旧id
//		String ExportId= "simple.jobstream.mapreduce.site.cnki_hy.ExoprtID";
//		JobNode ID= new JobNode("cnkihy_ID", defaultRootDir, 0, ExportId);
//		
//		result.add(StdXXXXObjectZhiTu);
//		StdXXXXObjectZhiTu.addChildJob(ID);
//		JobNode hydb = new JobNode("cnki_hy", defaultRootDir,
//				0, "simple.jobstream.mapreduce.site.cnki_hy.cnki_hydb");
//		hydb.setConfig("inputHdfsPath", "/RawData/cnki/hy/latest_bak_20190530");
//		hydb.setConfig("outputHdfsPath", "/user/qianjun/cnki_hy/zlf");
//		result.add(hydb);
//		result.add(StdXXXXObjectZhiTu);
//	
		
//		// 将A层xxx转换成智立方DB
//		String DBpathZLF = "/user/qianjun/cnki_hy/zlf";
//		String ZLF= "simple.jobstream.mapreduce.site.cnki_hy.StdXXXXObjectForZLF";
//		JobNode stdZLF = new JobNode("stdZLF", defaultRootDir, 0,ZLF);
//		stdZLF.setConfig("inputHdfsPath", latestDir);
//		stdZLF.setConfig("outputHdfsPath", DBpathZLF);
//		result.add(stdZLF);
//		result.add(test_oldToA);
//		test_oldToA.addChildJob(Std2Db3Atest);
//		Std2Db3Atest.addChildJob(StdXXXXObjectZhiTu);
//		StdXXXXObjectZhiTu.addChildJob(stdZLF);
//		return result;
		
		// 解析页面	
//		JobNode Std2Db3Atest = JobNodeModel.getJobNode4Std2Db3("cnki_hy.Std2Db3A", "simple.jobstream.mapreduce.common.vip.Std2Db3A",rawXXXXObjectDir, 
//		  stdDirA, 
//         "base_obj_meta_a_bs.cnki_hy", 
//         "/RawData/_rel_file/base_obj_meta_a_template_hy.db3", 
//          1);
//		result.add(Json2XXXXObject);
//		Json2XXXXObject.addChildJob(Std2Db3Atest);
		
		// 根据全文id提取智立方数据
		// 导出智图
		JobNode hydb_zlf_old = new JobNode("cnki_hy", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.cnki_hy.StdXXXXObjectForZLF_old");
		hydb_zlf_old.setConfig("inputHdfsPath", "/RawData/cnki/hy/latest");
		hydb_zlf_old.setConfig("outputHdfsPath", "/user/qianjun/cnki_hy/zlf");
		result.add(hydb_zlf_old);

		return result;
	}
}
