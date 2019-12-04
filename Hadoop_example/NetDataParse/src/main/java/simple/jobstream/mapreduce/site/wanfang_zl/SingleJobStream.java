package simple.jobstream.mapreduce.site.wanfang_zl;

import java.util.LinkedHashSet;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

import com.vipcloud.JobNode.JobNode;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		
		String rawHtmlDir = "/RawData/wanfang/zl/big_json/20190616,/RawData/wanfang/zl/big_json/20190625";//文件输入目录
		String rawXXXXObjectDir = "/RawData/wanfang/zl/xxxxobject";//新XXXXOBj 输出目录
		String latest_tempDir = "/RawData/wanfang/zl/latest_temp";	//新数据合并老数据后生成，新一轮的总数据存放地址
		String latestDir = "/RawData/wanfang/zl/latest";	//成品目录
		String newXXXXObjectDir = "/RawData/wanfang/zl/new_data/xxxxobject";//本次新增的数据（XXXXObject）
		String StdDirZT= "/RawData/wanfang/zl/new_data/std_zhitu";	 // 智图
		String StdDirA= "/RawData/wanfang/zl/new_data/std_A";//A层
		// 正常更新
		// 第一步：将最近增加的big_htm文件转换为xxxxobjcet文件存放在xxxxobject目录下
		JobNode Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("wanfang_zl.Json2XXXXObject",
		DateTimeHelper.getNowTimeAsBatch(), "simple.jobstream.mapreduce.site.wanfang_zl.Json2XXXXObject2",
		rawHtmlDir, rawXXXXObjectDir, 10);
		
		// 将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp = JobNodeModel.getJonNode4MergeXXXXObject("wanfang_zl.MergeXXXXObject2Temp",
				rawXXXXObjectDir, latestDir, latest_tempDir, 200);

//		// 生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject4Ref("wanfang_zl.GenNewData",rawXXXXObjectDir,
				latest_tempDir, newXXXXObjectDir, 200);
		
		// 生成A层数据
		JobNode Std2Db3A = JobNodeModel.getJobNode4Std2Db3("wanfang_zl.Std2Db3A", "simple.jobstream.mapreduce.common.vip.Std2Db3A",newXXXXObjectDir, 
				 StdDirA, 
         "base_obj_meta_a_bs.wanfang_zl", 
         "/RawData/_rel_file/base_obj_meta_a_template_zl.db3", 
          3);
		
		// 生成智图数据
		JobNode StdXXXXObjectZhiTu = JobNodeModel.getJobNode4Std2Db3("wanfang_zl.StdZhiTu",
				"simple.jobstream.mapreduce.site.wanfang_zl.StdXXXXObjectForZhiTu",newXXXXObjectDir,StdDirZT,
				"wanfang_zl", "/RawData/_rel_file/zt_template.db3", 3);
//
		// 备份累积数据
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("wanfang_zl.Temp2Latest", latest_tempDir,
				latestDir);
//		result.add(StdXXXXObjectZhiTu);
//		Json2XXXXObject.addChildJob(StdXXXXObjectZhiTu);
//		//正常更新
//		result.add(Json2XXXXObject);
//		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
//		MergeXXXXObject2Temp.addChildJob(GenNewData);
//		GenNewData.addChildJob(StdXXXXObjectZhiTu);
//		StdXXXXObjectZhiTu.addChildJob(Std2Db3A);
//		Std2Db3A.addChildJob(Temp2Latest);
		
//		JobNode countXXX = JobNodeModel.getJobNode4Std2Db3("wanfang_zl.StdZhiTu",
//				"simple.jobstream.mapreduce.site.wanfang_zl.StdXXXXObjectForZhiTu",latestDir,StdDirZT,
//				"wanfang_zl", "/RawData/_rel_file/zt_template.db3", 1);
//		result.add(countXXX);
//		result.add(Json2XXXXObject);
//		Json2XXXXObject.addChildJob(StdXXXXObjectZhiTu);
//		StdXXXXObjectZhiTu.addChildJob(Std2Db3A);
//		
		// 导出智图
//		JobNode Std = new JobNode("wanfangzl", defaultRootDir, 0,"simple.jobstream.mapreduce.site.wanfang_zl.StdXXXXObject_zt");
//		Std.setConfig("inputHdfsPath","/RawData/wanfang/zl/xxxxobject");
//		Std.setConfig("outputHdfsPath","/user/qianjun/wanfang_zl/db");

		
		//导出智立方
//		JobNode StdZLF = new JobNode("wanfangzl", defaultRootDir, 0,"simple.jobstream.mapreduce.site.wanfang_zl.StdXXXXObject");
//		StdZLF.setConfig("inputHdfsPath","/RawData/wanfang/zl/xxxxobject");
//		StdZLF.setConfig("outputHdfsPath","/user/qianjun/wanfang_zl/db");
//		result.add(StdZLF);
		
		// 将latest中xxxx转成A层xxxxx
//		String latestDirA = "/RawData/wanfang/zl/latestA";
//		String task = "simple.jobstream.mapreduce.site.wanfang_zl.ZLFXXXXObiectForA";
//		JobNode test_oldToA = new JobNode("oldChangA", defaultRootDir, 0,task);
//		test_oldToA.setConfig("inputHdfsPath", "/RawData/wanfang/zl/latest_bak_20190530");
//		test_oldToA.setConfig("outputHdfsPath", latestDirA);
//		result.add(test_oldToA);

		// 将A层直接转换成智图
//		String DBpath = "/RawData/wanfang/zl/new_data/std_zhitu";	
//		JobNode StdXXXXObjectZhiTu = JobNodeModel.getJobNode4Std2Db3("wanfang_zl.StdZhiTu",
//				"simple.jobstream.mapreduce.site.wanfang_zl.StdXXXXObjectForZhiTu","/RawData/wanfang/zl/test", DBpath,
//				"wanfang_zl", "/RawData/_rel_file/zt_template.db3", 1);
//		
////		return result;
//
//		
//		// 将A层直接转换成智立方
		String DBpathZLF = "/user/ganruoxun/Temp_DB3/wanfang_zl";
		String ZLF= "simple.jobstream.mapreduce.site.wanfang_zl.StdXXXXObjectZLF";
		JobNode stdZLF = new JobNode("stdZLF", defaultRootDir, 0,ZLF);
		stdZLF.setConfig("inputHdfsPath", "/RawData/wanfang/zl/latest");
		stdZLF.setConfig("outputHdfsPath", DBpathZLF);
		result.add(stdZLF);
		
//		result.add(StdXXXXObjectZhiTu);
//		StdXXXXObjectZhiTu.addChildJob(stdZLF);
//		result.add(stdZLF);
//		return result;
		
		// 将A层xxx导出DB3查看/RawData/cnki/bs/test /user/qianjun/cnki_bs/baseDB /user/qianjun/cnki_bs/output
//		JobNode Std2Db3Atest = JobNodeModel.getJobNode4Std2Db3("wanfang_zl.Std2Db3A", "simple.jobstream.mapreduce.common.vip.Std2Db3A","/RawData/wanfang/zl/test", 
//				  "/user/qianjun/wanfang_zl/output", 
//		             "base_obj_meta_a_bs.wanfang_zl", 
//		             "/RawData/_rel_file/base_obj_meta_a_template_zl.db3", 
//		              1);
//		result.add(Std);

		return result;
	}
}
