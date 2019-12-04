package simple.jobstream.mapreduce.site.ebsco_latest;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		String rootDir = "";
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		
		String rawHtmlDir = "/RawData/ebsco/Ebsco_new/big_html/2019_buh/20190522";
		String rawXXXXObjectDir = "/RawData/ebsco/Ebsco_new/XXXXObject";
		String latest_tempDir = "/RawData/ebsco/Ebsco_new/latest_temp"; // 临时成品目录
		String latestDir = "/RawData/ebsco/Ebsco_new/latest"; // 成品目录
		String newXXXXObjectDir = "/RawData/ebsco/Ebsco_new/new_data/XXXXObject"; // 本次新增的数据（XXXXObject）
		String std_Dir_zt = "/RawData/ebsco/Ebsco_new/new_data/zt/StdEBSCO";
		String std_Dir_a = "/RawData/ebsco/Ebsco_new/new_data/a/StdEBSCO";
		// String testDir = "/user/qianjun/ebsco";
		
		JobNode Html2XXXXObject = new JobNode("EbscoJournalParse", rootDir, 0,
				"simple.jobstream.mapreduce.site.ebsco.Html2XXXXObject");
		Html2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
		Html2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);

		// 将历史累积数据和新数据合并去重
//
		JobNode MergeXXXXObject2Temp = JobNodeModel.getJonNode4MergeXXXXObject("ebsco.MergeXXXXObject2Temp",
				rawXXXXObjectDir, latestDir, latest_tempDir, 200);

//		// 生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("ebsco.GenNewData", 
				rawXXXXObjectDir, latest_tempDir,newXXXXObjectDir, 200);	
			

		 //导出数据到 db3
		JobNode StdNew = JobNodeModel.getJobNode4Std2Db3("ebsco.Std","simple.jobstream.mapreduce.site.ebsco.StdXXXXobject_zt", 
				newXXXXObjectDir, std_Dir_zt, "ebscobuhjournal_zt","/RawData/_rel_file/zt_template.db3",1);
//		//测试 导出数据到 db3
//		JobNode StdNew = JobNodeModel.getJobNode4Std2Db3("ebsco.Std","simple.jobstream.mapreduce.site.ebsco.StdXXXXobject_zt", 
//				rawXXXXObjectDir, std_Dir, "ebscobuhJournal_zt","/RawData/_rel_file/zt_template.db3",1);

//		// 备份累积数据
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("ebsco.Temp2Latest", latest_tempDir, latestDir);
		
		//导出数据到a层
		JobNode Std2Db3A = JobNodeModel.getJobNode4Std2Db3("ebsco.Std2Db3A", "simple.jobstream.mapreduce.site.ebsco.StdXXXXobject_a", newXXXXObjectDir, 
				std_Dir_a, 
		             "ebscobuhjournal_a", 
		             "/RawData/_rel_file/base_obj_meta_a_template_qkwx.db3", 
		              1);
		
		// 测试  导出数据到a层
//		JobNode Std2Db3A = JobNodeModel.getJobNode4Std2Db3("ebsco.Std2Db3A", "simple.jobstream.mapreduce.site.ebsco.StdXXXXobject_a", rawXXXXObjectDir, 
//				std_Dir, 
//		             "ebscobuhjournal_a", 
//		             "/RawData/_rel_file/base_obj_meta_a_template_qkwx.db3", 
//		              1);
		
		// 提出数据
		// 导出数据到 db3
//		JobNode StdNew = JobNodeModel.getJobNode4Std2Db3("ebsco.Std","simple.jobstream.mapreduce.site.ebsco.ExportEbscoDb3", 
//				latestDir, testDir, "ebsco","/RawData/_rel_file/zt_template.db3",1);

	

		// 正常更新
		result.add(Html2XXXXObject);
		Html2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdNew);
		StdNew.addChildJob(Temp2Latest);
		Temp2Latest.addChildJob(Std2Db3A);
		
		//测试
//		result.add(Html2XXXXObject);
//		Html2XXXXObject.addChildJob(StdNew);
		//导出到a
//		result.add(Std2Db3A);
		
		// 提出lastest中数据
//		result.add(StdNew);
//		
//		


		return result;
	}
}
