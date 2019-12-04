package simple.jobstream.mapreduce.site.cnki_zl;

import java.util.LinkedHashSet;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

import com.vipcloud.JobNode.JobNode;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir="";
		String rawHtmlDir = "/RawData/cnki/zl/big_htm/20190616,/RawData/cnki/zl/big_htm/20190618,/RawData/cnki/zl/big_htm/20190619";//文件输入目录
		String rawXXXXObjectDir = "/RawData/cnki/zl/xxxxobject";//新XXXXOBj 输出目录
		String latest_tempDir = "/RawData/cnki/zl/latest_temp";	//新数据合并老数据后生成，新一轮的总数据存放地址
		String latestDir = "/RawData/cnki/zl/latest";	//成品目录
		String newXXXXObjectDir = "/RawData/cnki/zl/new_data/xxxxobject";//本次新增的数据（XXXXObject）
		String stdDirZT = "/RawData/cnki/zl/new_data/std_zhitu";//智图DB3目录
		String stdDirA = "/RawData/cnki/zl/new_data/std_A";//A层DB3目录
		
		// 正常更新
		// 第一步：将最近增加的big_htm文件转换为xxxxobjcet文件存放在xxxxobject目录下
		JobNode Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("cnki_zl.Json2XXXXObject",
		DateTimeHelper.getNowTimeAsBatch(), "simple.jobstream.mapreduce.site.cnki_zl.Json2XXXXObject",
		rawHtmlDir, rawXXXXObjectDir, 10);

		// 将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp = JobNodeModel.getJonNode4MergeXXXXObject("cnki_zl.MergeXXXXObject2Temp",
				rawXXXXObjectDir, latestDir, latest_tempDir, 200);

//		// 生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject4Ref("cnki_zl.GenNewData", rawXXXXObjectDir,
				latest_tempDir, newXXXXObjectDir, 200);
//
		// 备份累积数据
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("cnki_zl.Temp2Latest", latest_tempDir,
				latestDir);
		
		// 导出智图
		JobNode StdXXXXObjectZhiTu = JobNodeModel.getJobNode4Std2Db3("cnki_zl.StdZhiTu",
				"simple.jobstream.mapreduce.site.cnki_zl.StdXXXXObjectForZhiTu",rawXXXXObjectDir,stdDirZT,
				"cnki_zl", "/RawData/_rel_file/zt_template.db3", 1);
		
		// 导出A层
		JobNode Std2Db3Atest = JobNodeModel.getJobNode4Std2Db3("cnki_zl.Std2Db3A", "simple.jobstream.mapreduce.common.vip.Std2Db3A",rawXXXXObjectDir, 
		  stdDirA, 
         "base_obj_meta_a_bs.cnki_zl", 
         "/RawData/_rel_file/base_obj_meta_a_template_zl.db3", 
          1);
		
//		String DBpath = "/RawData/cnki/zl/new_data/std_zhitu";	
//		JobNode StdXXXXObjectZhiTu = JobNodeModel.getJobNode4Std2Db3("cnki_zl.StdZhiTu",
//				"simple.jobstream.mapreduce.site.cnki_zl.StdXXXXObject_zt","/RawData/cnki/zl/xxxxobject","/user/qianjun/cnki_zl",
//				"cnki_zl", "/RawData/_rel_file/zt_template.db3", 1);
		
		// 正常更新流程
		result.add(Json2XXXXObject);
		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdXXXXObjectZhiTu);
		StdXXXXObjectZhiTu.addChildJob(Std2Db3Atest);
		Std2Db3Atest.addChildJob(Temp2Latest);
//		result.add(StdXXXXObjectZhiTu);
//		
		
//		JobNode Html2XXXXObject = new JobNode("RscParse", defaultRootDir,0, "simple.jobstream.mapreduce.site.naturejournal.json2xobj");
//		Html2XXXXObject.setConfig("inputHdfsPath","/RawData/nature/big_json/20181218");
//		Html2XXXXObject.setConfig("outputHdfsPath","/RawData/nature/xxobj");
//		
//		JobNode Std = new JobNode("wanfangzl", defaultRootDir, 0,"simple.jobstream.mapreduce.site.wanfang_zl.StdXXXXObject_zt");
//		Std.setConfig("inputHdfsPath","/RawData/wanfang/zl/latest_temp");
//		Std.setConfig("outputHdfsPath","/RawData/wanfang/zl/new_data/stdZL_zt");
//		
//		
//		JobNode StdDb3 = JobNodeModel.getJobNode4Std2Db3("cnkiZL","simple.jobstream.mapreduce.site.cnki_zl.StdXXXXObject", 
//				inpput, output, "wanfangzl","/RawData/_rel_file/zt_template.db3",100);
//		
//		// 第一步：将最近增加的big_htm文件转换为xxxxobjcet文件存放在xxxxobject目录下
//		JobNode Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("naturejournal.Json2XXXXObject",
//		   DateTimeHelper.getNowTimeAsBatch(), "simple.jobstream.mapreduce.site.naturejournal.Json2XXXXObject",
//		   "/RawData/nature/big_json/20181218", "/RawData/nature/xxobj_a", 40);
//		
//		获取cnkizlID
//		JobNode ParseCnkiID = new JobNode("wanfangzl", defaultRootDir, 0,"simple.jobstream.mapreduce.site.cnki_zl.ParseCnkizlId");
//		result.add(ParseCnkiID);

////		Html2XXXXObject.addChildJob(Std);
//		
////		Std2Db3A.addChildJob(Html2XXXXObject);
////		Html2XXXXObject.addChildJob(Std);
//		return result;
		
		// 将latest中xxxx转成A层xxxxx
//		String latestDirA = "/RawData/cnki/zl/latestA";
//		String task = "simple.jobstream.mapreduce.site.cnki_zl.ZLFXXXXObiectForA";
//		JobNode test_oldToA = new JobNode("oldChangA", defaultRootDir, 0,task);
//		test_oldToA.setConfig("inputHdfsPath", "/RawData/cnki/zl/latest");
//		test_oldToA.setConfig("outputHdfsPath", latestDirA);
		
		// 将A层xxx导出A层格式
//		JobNode Std2Db3Atest = JobNodeModel.getJobNode4Std2Db3("cnki_zl.Std2Db3A", "simple.jobstream.mapreduce.common.vip.Std2Db3A",latestDirA, 
//				  "/RawData/cnki/zl/new_data/std_A", 
//		             "base_obj_meta_a_bs.cnki_zl", 
//		             "/RawData/_rel_file/base_obj_meta_a_template_zl.db3", 
//		              50);
//		
//		// 将A层XXX转换成智图
//		String DBpath = "/RawData/cnki/zl/new_data/std_zhitu";	
//		JobNode StdXXXXObjectZhiTu = JobNodeModel.getJobNode4Std2Db3("cnki_zl.StdZhiTu",
//				"simple.jobstream.mapreduce.site.cnki_zl.StdXXXXObject_zt","/RawData/cnki/zl/xxxxobject","/user/qianjun/cnki_zl",
//				"cnki_zl", "/RawData/_rel_file/zt_template.db3", 1);
//		

		
		// 将A层xxx转换成智立方
//		String DBpathZLF = "/RawData/cnki/zl/new_data/std_zlf";
//		String ZLF= "simple.jobstream.mapreduce.site.cnki_zl.StdXXXXObjectForZLF";
//		JobNode stdZLF = new JobNode("stdZLF", defaultRootDir, 0,ZLF);
//		stdZLF.setConfig("inputHdfsPath", latestDirA);
//		stdZLF.setConfig("outputHdfsPath", DBpathZLF);
		
//		result.add(test_oldToA);
//		test_oldToA.addChildJob(Std2Db3Atest);
//		Std2Db3Atest.addChildJob(StdXXXXObjectZhiTu);
//		StdXXXXObjectZhiTu.addChildJob(stdZLF);
		// 将latestA转换成A层格式DB
//		result.add(test_oldToA);
//		test_oldToA.addChildJob(Std2Db3Atest);
//		
//		Std2Db3Atest.addChildJob(StdXXXXObjectZhiTu);
		
		// 解析临时文件
//		JobNode Html2XXXXObject = new JobNode("CnkiParse", defaultRootDir,0, "simple.jobstream.mapreduce.site.cnki_zl.Html2xxxxobject");
//		Html2XXXXObject.setConfig("inputHdfsPath","/RawData/cnki/zl/big_htm/20190616");
//		Html2XXXXObject.setConfig("outputHdfsPath","/RawData/cnki/zl/xxxxobject");
//		
//		JobNode Std = new JobNode("cnkizl", defaultRootDir, 0,"simple.jobstream.mapreduce.site.cnki_zl.StdXXXXObject_zt");
//		Std.setConfig("inputHdfsPath","/RawData/cnki/zl/xxxxobject");
//		Std.setConfig("outputHdfsPath","/user/qianjun/cnki_zl");
//		result.add(Std);
//		
//		
//		result.add(Html2XXXXObject);
//		Html2XXXXObject.addChildJob(StdXXXXObjectZhiTu);

		return result;
		
	}
}
