package simple.jobstream.mapreduce.site.wanfang_hy;

import java.util.LinkedHashSet;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

import com.vipcloud.JobNode.JobNode;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		
		String rawHtmlDir = "/RawData/wanfang/hy/big_htm/big_htm_20190618";//文件输入目录
		String rawXXXXObjectDir = "/RawData/wanfang/hy/XXXXObject";//新XXXXOBj 输出目录
		String latest_tempDir = "/RawData/wanfang/hy/latest_temp";	//新数据合并老数据后生成，新一轮的总数据存放地址
		String latestDir = "/RawData/wanfang/hy/latest";	//成品目录
		String newXXXXObjectDir = "/RawData/wanfang/hy/new_data/XXXXObject";//本次新增的数据（XXXXObject）
		String StdDirZT= "/RawData/wanfang/hy/new_data/std_zhitu";	 // 智图
		String StdDirA= "/RawData/wanfang/hy/new_data/std_A";//A层
		// 正常更新
		// 第一步：将最近增加的big_htm文件转换为xxxxobjcet文件存放在xxxxobject目录下
		JobNode Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("wanfang_hy.Json2XXXXObject",
		DateTimeHelper.getNowTimeAsBatch(), "simple.jobstream.mapreduce.site.wanfang_hy.Json2XXXXObject",
		rawHtmlDir, rawXXXXObjectDir, 10);
		
		// 将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp = JobNodeModel.getJonNode4MergeXXXXObject("wanfang_hy.MergeXXXXObject2Temp",
				rawXXXXObjectDir, latestDir, latest_tempDir, 200);

//		// 生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject4Ref("wanfang_hy.GenNewData", rawXXXXObjectDir,
				latest_tempDir, newXXXXObjectDir, 200);
		
		// 生成A层数据
		JobNode Std2Db3A = JobNodeModel.getJobNode4Std2Db3("wanfang_hy.Std2Db3A", "simple.jobstream.mapreduce.common.vip.Std2Db3A",newXXXXObjectDir, 
				 StdDirA, 
         "base_obj_meta_a_bs.wanfang_hy", 
         "/RawData/_rel_file/base_obj_meta_a_template_hy.db3", 
          1);
		
		// 生成智图数据
		JobNode StdXXXXObjectZhiTu = JobNodeModel.getJobNode4Std2Db3("wanfang_hy.StdZhiTu",
				"simple.jobstream.mapreduce.site.wanfang_hy.stdXXXXobjForZT",newXXXXObjectDir,StdDirZT,
				"wanfang_hy", "/RawData/_rel_file/zt_template.db3", 1);
//
		// 备份累积数据
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("wanfang_hy.Temp2Latest", latest_tempDir,
				latestDir);
//		result.add(Json2XXXXObject);
//		Json2XXXXObject.addChildJob(StdXXXXObjectZhiTu);
//		//正常更新
//		result.add(Json2XXXXObject);
//		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
//		MergeXXXXObject2Temp.addChildJob(GenNewData);
//		GenNewData.addChildJob(StdXXXXObjectZhiTu);
//		StdXXXXObjectZhiTu.addChildJob(Std2Db3A);
//		Std2Db3A.addChildJob(Temp2Latest);
//		Json2XXXXObject.addChildJob(MergeIEEEXXXXObject2Temp);
//		MergeIEEEXXXXObject2Temp.addChildJob(GenNewData);
//		GenNewData.addChildJob(Stdhy);
//		//StdEI.addChildJob(Temp2Latest);
//		

//		//导出完整数据	
//		Stdhy.setConfig("inputHdfsPath", latestDir);
//		Stdhy.setConfig("outputHdfsPath", stdhyDir);
//		result.add(Stdhy);
		
		// 将latest中xxxx转成A层xxxxx
//		String latestDirA = "/RawData/wanfang/hy/latestA";
//		String task = "simple.jobstream.mapreduce.site.wanfang_hy.ZLFXXXXObiectForA";
//		JobNode test_oldToA = new JobNode("oldChangA", defaultRootDir, 0,task);
//		test_oldToA.setConfig("inputHdfsPath", "/RawData/wanfang/hy/latest");
//		test_oldToA.setConfig("outputHdfsPath", latestDirA);
//		
//		JobNode Std2Db3Atest = JobNodeModel.getJobNode4Std2Db3("wanfang_hy.Std2Db3A", "simple.jobstream.mapreduce.common.vip.Std2Db3A","/RawData/wanfang/hy/latestA", 
//		  "/RawData/wanfang/hy/new_data/std_A", 
//           "base_obj_meta_a_bs.wanfang_hy", 
//           "/RawData/_rel_file/base_obj_meta_a_template_hy.db3", 
//            10);
//		
//		// 将A层XXX转换成智图
//		String DBpath = "/RawData/wanfang/hy/new_data/std_zhitu";	
//		JobNode StdXXXXObjectZhiTu = JobNodeModel.getJobNode4Std2Db3("wanfang_hy.StdZhiTu",
//				"simple.jobstream.mapreduce.site.wanfang_hy.stdXXXXobjForZT",latestDirA, DBpath,
//				"wanfang_hy", "/RawData/_rel_file/zt_template.db3", 10);
//		result.add(StdXXXXObjectZhiTu);
//		
//		// 将A层xxx转换成智立方
		String DBpathZLF = "/user/ganruoxun/Temp_DB3/wanfang_hy";
		String ZLF= "simple.jobstream.mapreduce.site.wanfang_hy.stdXXXXobjForZLF";
		JobNode stdZLF = new JobNode("stdZLF", defaultRootDir, 0,ZLF);
		stdZLF.setConfig("inputHdfsPath", latestDir);
		stdZLF.setConfig("outputHdfsPath", DBpathZLF);
		result.add(stdZLF);
		
//		result.add(test_oldToA);
//		test_oldToA.addChildJob(Std2Db3Atest);
//		Std2Db3Atest.addChildJob(StdXXXXObjectZhiTu);
//		StdXXXXObjectZhiTu.addChildJob(stdZLF);
		return result;

//		
	}
}
