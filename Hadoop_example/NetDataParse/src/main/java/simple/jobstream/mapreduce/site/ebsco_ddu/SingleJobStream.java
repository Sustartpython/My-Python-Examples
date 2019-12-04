package simple.jobstream.mapreduce.site.ebsco_ddu;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		String rootDir = "";
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		
		String rawHtmlDir = "/RawData/ebsco/ddu_OpenDisserations/big_html/20190711";
		String rawXXXXObjectDir = "/RawData/ebsco/ddu_OpenDisserations/XXXXObject";
		String latest_tempDir = "/RawData/ebsco/ddu_OpenDisserations/latest_temp"; // 临时成品目录
		String latestDir = "/RawData/ebsco/ddu_OpenDisserations/latest"; // 成品目录
		String newXXXXObjectDir = "/RawData/ebsco/ddu_OpenDisserations/new_data/XXXXObject"; // 本次新增的数据（XXXXObject）
		String std_Dir_zt = "/RawData/ebsco/ddu_OpenDisserations/new_data/zt/StdEBSCO";
		String std_Dir_a = "/RawData/ebsco/ddu_OpenDisserations/new_data/a/StdEBSCO";
		String std="/RawData/ebsco/ddu_OpenDisserations/latest_XXXXObject";
		
		JobNode Html2XXXXObject = new JobNode("ebsco_ddu_Parse", rootDir, 0,
				"simple.jobstream.mapreduce.site.ebsco_ddu.Html2XXXXObject");
		Html2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
		Html2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);

		// 将历史累积数据和新数据合并去重
//
		JobNode MergeXXXXObject2Temp = JobNodeModel.getJonNode4MergeXXXXObject("ebsco_ddu_MergeXXXXObject2Temp",
				rawXXXXObjectDir, latestDir, latest_tempDir, 200);

//		// 生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("ebsco_ddu_GenNewData", 
				rawXXXXObjectDir, latest_tempDir,newXXXXObjectDir, 200);	
			

		 //导出数据到 db3
		JobNode StdNew = JobNodeModel.getJobNode4Std2Db3("ebsco_ddu_zt","simple.jobstream.mapreduce.site.ebsco_ddu.StdXXXXobject_zt", 
				newXXXXObjectDir, std_Dir_zt, "zt_ebscodduthesis","/RawData/_rel_file/zt_template.db3",1);

//		// 备份累积数据
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("ebsco_ddu_Temp2Latest", latest_tempDir, latestDir);
		
		//导出数据到a层
		JobNode Std2Db3A = JobNodeModel.getJobNode4Std2Db3("ebsco_ddu_a", "simple.jobstream.mapreduce.site.ebsco_ddu.StdXXXXobject_a", newXXXXObjectDir, 
				std_Dir_a, 
		             "a_ebscodduthesis", 
		             "/RawData/_rel_file/base_obj_meta_a_template_qkwx.db3", 
		              1);
		//测试
//		JobNode StdNew2 = JobNodeModel.getJobNode4Std2Db3("ebsco_ddu_zt","simple.jobstream.mapreduce.site.ebsco_ddu.StdXXXXobject_zt", 
//				rawXXXXObjectDir, std_Dir_zt, "zt_ebscodduthesis","/RawData/_rel_file/zt_template.db3",1);
//		JobNode Std2Db3A2 = JobNodeModel.getJobNode4Std2Db3("ebsco_ddu_a", "simple.jobstream.mapreduce.site.ebsco_ddu.StdXXXXobject_a", rawXXXXObjectDir, 
//				std_Dir_a, 
//		             "a_ebscodduthesis", 
//		             "/RawData/_rel_file/base_obj_meta_a_template_qkwx.db3", 
//		              1);
//		result.add(Html2XXXXObject);
////		result.add(Std2Db3A2);
//		Html2XXXXObject.addChildJob(StdNew2);
//		StdNew2.addChildJob(Std2Db3A2);
		

		// 正常更新
		result.add(Html2XXXXObject);
		Html2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdNew);
		StdNew.addChildJob(Temp2Latest);
//		Temp2Latest.addChildJob(Std2Db3A);
		
		//合并新旧latest，并生成新的db3
//		JobNode MergeXXXXObject2Temp_latest = JobNodeModel.getJonNode4MergeXXXXObject("ebsco.MergeXXXXObject2Temp",
//				"/RawData/ebsco/a9h_ASC/latest_XXXXObject", latestDir, latest_tempDir, 200);
//		JobNode Temp2Latest_latest = JobNodeModel.getJobNode4CopyXXXXObject("ebsco.Temp2Latest", latest_tempDir, latestDir);
//		JobNode StdNew_latest = JobNodeModel.getJobNode4Std2Db3("ebsco.Std","simple.jobstream.mapreduce.site.ebsco_a9h.StdXXXXobject_zt", 
//				std, std_Dir_zt, "ebscoa9hjournal_zt","/RawData/_rel_file/zt_template.db3",10);
//		JobNode Std2Db3A_latest = JobNodeModel.getJobNode4Std2Db3("ebsco.Std2Db3A", "simple.jobstream.mapreduce.site.ebsco_a9h.StdXXXXobject_a", latestDir, 
//				std_Dir_a, 
//		             "ebscoa9hjournal_a", 
//		             "/RawData/_rel_file/base_obj_meta_a_template_qkwx.db3", 
//		              10);
//		result.add(MergeXXXXObject2Temp_latest);
//		MergeXXXXObject2Temp_latest.addChildJob(Temp2Latest_latest);
//		Temp2Latest_latest.addChildJob(StdNew_latest);
//		StdNew_latest.addChildJob(Std2Db3A_latest);
//		String ExportId= "simple.jobstream.mapreduce.site.ebsco_a9h.ExoprtID";
//		JobNode ID= new JobNode("ebsco_a9h_ID",rootDir, 0, ExportId);
//		Std2Db3A_latest.addChildJob(ID);
//		result.add(StdNew_latest);
//		StdNew_latest.addChildJob(ID);
		return result;
	}
}
