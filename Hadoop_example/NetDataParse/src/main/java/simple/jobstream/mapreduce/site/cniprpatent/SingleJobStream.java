package simple.jobstream.mapreduce.site.cniprpatent;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> cniprPatent(){
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		
		String rawCoverDir = "/RawData/cniprpatent/big_cover/json";
		String coverXXXXObjectDir = "/RawData/cniprpatent/CoverXXXXObject";
		String rawHtmlDir2 = "/RawData/cniprpatent/big_json_pub/20181128";
		String rawXXXXObjectDir2 = "/RawData/cniprpatent/XXXXObject_pub";
		String latest_tempDirCover2 = "/RawData/cniprpatent/latest_temp_cover_pub";
		String newXobjDir = "/RawData/cniprpatent/new_data";
		String db3Dir = "/RawData/cniprpatent/Stdpatent";
		String latestDir = "/RawData/cniprpatent/latest";	//成品目录
		String latest_tempDir = "/RawData/cniprpatent/latest_temp";	//临时成品目录
		
		String rawHtmlDir21 = "/RawData/cniprpatent/big_json_pub/20180828";
		String rawHtmlDir22 = "/RawData/cniprpatent/big_json_pub/20180910";
		String rawHtmlDir23 = "/RawData/cniprpatent/big_json_pub/20180918";
		String rawHtmlDir24 = "/RawData/cniprpatent/big_json_pub/20180921";
		String rawHtmlDir25 = "/RawData/cniprpatent/big_json_pub/20181128";
		
		String allrawHtml = rawHtmlDir21+","+rawHtmlDir22+","+rawHtmlDir23+","+rawHtmlDir24+","+rawHtmlDir25;
		
		//Pub
		/**/
		JobNode Cover2XXXXObject = new JobNode("CniprPatentParse", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.cniprpatent.sipoCover2XXXXObject");		
		Cover2XXXXObject.setConfig("inputHdfsPath", rawCoverDir);
		Cover2XXXXObject.setConfig("outputHdfsPath", coverXXXXObjectDir);
		
		
		//公布公告
//		JobNode html2XXXXObject2 = new JobNode("CniprPatentParse", defaultRootDir,
//				0, "simple.jobstream.mapreduce.site.cniprpatent.sipoJson2XXXXObjectPub");		
//		html2XXXXObject2.setConfig("inputHdfsPath", allrawHtml);   //rawHtmlDir2
//		html2XXXXObject2.setConfig("outputHdfsPath", rawXXXXObjectDir2);
		
		//公布公告
		JobNode html2XXXXObject2 =JobNodeModel.getJobNode4Parse2XXXXObject(
				"CniprPatentParse",
				DateTimeHelper.getNowTimeAsBatch(),
				"simple.jobstream.mapreduce.site.cniprpatent.sipoJson2XXXXObjectPub",
				allrawHtml,   //rawHtmlDir2
				rawXXXXObjectDir2,
				100);
		
		
		JobNode MergeXXXXObject2TempCover2  = new JobNode("CniprPatentParse", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.cniprpatent.MergeXXXXObject2CoverXobj");
		MergeXXXXObject2TempCover2.setConfig("inputHdfsPath", rawXXXXObjectDir2 + "," + coverXXXXObjectDir);
		MergeXXXXObject2TempCover2.setConfig("outputHdfsPath", latest_tempDirCover2);
		
		
//		JobNode Std2DbZTCnipr = new JobNode("Std2DbZTCnipr", defaultRootDir,
//				0, "simple.jobstream.mapreduce.site.cniprpatent.Std2DbZTCnipr");		
//		Std2DbZTCnipr.setConfig("inputHdfsPath", newXobjDir);
//		Std2DbZTCnipr.setConfig("outputHdfsPath", db3Dir);
//		Std2DbZTCnipr.setConfig("reduceNum", "1");
		
		JobNode Std2DbZTCnipr = JobNodeModel.getJobNode4Std2Db3("Std2DbZTCnipr",
				"simple.jobstream.mapreduce.site.cniprpatent.Std2DbZTCnipr", 
				newXobjDir, 
				db3Dir, 
				"CniprPatent",
				"/RawData/_rel_file/zt_template.db3",
				200);
		
		
//		JobNode Std2DbZTCniprCover = new JobNode("Std2DbZTCnipr", defaultRootDir,
//				0, "simple.jobstream.mapreduce.site.cniprpatent.Std2DbZTCniprCover");		
//		Std2DbZTCnipr.setConfig("inputHdfsPath", latest_tempDirCover2);
//		Std2DbZTCnipr.setConfig("outputHdfsPath", db3Dir);
//		Std2DbZTCnipr.setConfig("reduceNum", "1");
//		
		JobNode Std2DbZTCniprCover = JobNodeModel.getJobNode4Std2Db3("Std2DbZTCniprCover",
				"simple.jobstream.mapreduce.site.cniprpatent.Std2DbZTCniprCover", 
				latest_tempDirCover2, 
				db3Dir, 
				"CniprPatent",
				"/RawData/_rel_file/zt_template.db3",
				100);
		
		
		JobNode MergeXXXXObject2Temp2 = JobNodeModel.getJonNode4MergeXXXXObject(
				"cnipr.MergeXXXXObject2Temp",
				latest_tempDirCover2, 
				latestDir, 
				latest_tempDir, 
				200);
		
		
		JobNode GenNewData2 = JobNodeModel.getJonNode4ExtractXXXXObject("cnipr.GenNewData", 
				latest_tempDirCover2, 
				latest_tempDir, 
				newXobjDir, 
				200);	
		
		
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("cnipr.Temp2Latest", 
				latest_tempDir, 
				latestDir);
		
		////////////////////////////////////////////////////////////
//		//第一步
//		//公布公告更新流程
//		// 解析图片路径
//		result.add(Cover2XXXXObject);
//		//公布公告解析
//		Cover2XXXXObject.addChildJob(html2XXXXObject2);
//		//公布公告图片合并
//		html2XXXXObject2.addChildJob(MergeXXXXObject2TempCover2);
//		//这里需要将db3中的图片路径改为图片的url 方便下载
//		//生成db3 得到图片的url 进行下载
//		MergeXXXXObject2TempCover2.addChildJob(Std2DbZTCniprCover);
		//下载的db3导入到cover表进行图片下载
		//导出db3 下载图片 然后上传到 rawCoverDir 记得改回db3的图片路径输出
		/////////////////////////////////////////////////////////////
		//第二步
		/*
		// 解析图片路径
		result.add(Cover2XXXXObject);
		//公布公告解析
		Cover2XXXXObject.addChildJob(MergeXXXXObject2TempCover2);
		//公布公告图片合并
		MergeXXXXObject2TempCover2.addChildJob(MergeXXXXObject2Temp2);
		//获取新的数据
		MergeXXXXObject2Temp2.addChildJob(GenNewData2);
		//db3生成
		GenNewData2.addChildJob(Std2DbZTCnipr);
		//合并
		Std2DbZTCnipr.addChildJob(Temp2Latest);
		*/
		
		///////////////////////////////////////////////////////////////////////////////
		//
		//
		//////////////////////////////////////////////////////////////////////////////
		
		
		
		String rawHtmlDir3 = "/RawData/cniprpatent/big_json_pss/20181204";
		String rawXXXXObjectDir3 = "/RawData/cniprpatent/XXXXObject_pss";
		String latest_tempDirCover3 = "/RawData/cniprpatent/latest_temp_cover_pss";
		
		//专利检索及分析
		//JobNode html2XXXXObject3 = new JobNode("CniprPatentParse", defaultRootDir,
		//		0, "simple.jobstream.mapreduce.site.cniprpatent.sipoJson2XXXXObjectPss");		
		//html2XXXXObject3.setConfig("inputHdfsPath", rawHtmlDir3);
		//html2XXXXObject3.setConfig("outputHdfsPath", rawXXXXObjectDir3);
		
		
		JobNode html2XXXXObject3 =JobNodeModel.getJobNode4Parse2XXXXObject(
				"CniprPatentParse",
				DateTimeHelper.getNowTimeAsBatch(),
				"simple.jobstream.mapreduce.site.cniprpatent.sipoJson2XXXXObjectPss",
				rawHtmlDir3,
				rawXXXXObjectDir3,
				100);
		
		JobNode MergeXXXXObject2TempCover3  = new JobNode("CniprPatentParse", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.cniprpatent.MergeXXXXObject2CoverXobj");
		MergeXXXXObject2TempCover3.setConfig("inputHdfsPath", rawXXXXObjectDir3 + "," + coverXXXXObjectDir);
		MergeXXXXObject2TempCover3.setConfig("outputHdfsPath", latest_tempDirCover3);
		
		

		JobNode MergeXXXXObject2Temp3 = JobNodeModel.getJonNode4MergeXXXXObject(
				"cnipr.MergeXXXXObject2Temp",
				latest_tempDirCover3, 
				latestDir, 
				latest_tempDir, 
				200);
		
		
		JobNode GenNewData3 = JobNodeModel.getJonNode4ExtractXXXXObject(
				"cnipr.GenNewData", 
				latest_tempDirCover3, 
				latest_tempDir, 
				newXobjDir, 
				200);	
		/*
		//更新 专利检索及分析 到总数居流程
		result.add(Cover2XXXXObject);
		Cover2XXXXObject.addChildJob(html2XXXXObject3);
		html2XXXXObject3.addChildJob(MergeXXXXObject2TempCover3);
		//这里需要将db3中的图片路径改为图片的url 方便下载
		MergeXXXXObject2TempCover3.addChildJob(Std2DbZTCniprCover);
		*/
		//下载的db3导入到cover表进行图片下载
		//导出db3 下载图片 然后上传到 rawCoverDir 记得改回db3的图片路径输出
		//第二步
		/*
		result.add(Cover2XXXXObject);
		Cover2XXXXObject.addChildJob(MergeXXXXObject2TempCover3);
		MergeXXXXObject2TempCover3.addChildJob(MergeXXXXObject2Temp3);
		MergeXXXXObject2Temp3.addChildJob(GenNewData3);
		GenNewData3.addChildJob(Std2DbZTCnipr);
		Std2DbZTCnipr.addChildJob(Temp2Latest);
		*/
//		//更新完成
		
		///////////////////////////////////////////////////////////////////////////////////////
		//
		//
		//////////////////////////////////////////////////////////////////////////////////////
		
		String rawHtmlDir = "/RawData/cniprpatent/big_json_jss/20181029";
		
		String rawXXXXObjectDir = "/RawData/cniprpatent/XXXXObject_jss";
		
		String latest_tempDirCover = "/RawData/cniprpatent/latest_temp_cover_jss";
		
		
		
		
		/**/
		//江苏所
		//JobNode html2XXXXObject = new JobNode("CniprPatentParse", defaultRootDir,
		//		0, "simple.jobstream.mapreduce.site.cniprpatent.sipoJson2XXXXObjectJss");		
		//html2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
		//html2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);
		
		JobNode html2XXXXObject =JobNodeModel.getJobNode4Parse2XXXXObject(
				"CniprPatentParse",
				DateTimeHelper.getNowTimeAsBatch(),
				"simple.jobstream.mapreduce.site.cniprpatent.sipoJson2XXXXObjectJss",
				rawHtmlDir,
				rawXXXXObjectDir,
				100);
		
		

		JobNode MergeXXXXObject2TempCover  = new JobNode(
				"CniprPatentParse", 
				defaultRootDir,
				0, 
				"simple.jobstream.mapreduce.site.cniprpatent.MergeXXXXObject2CoverXobj");
		MergeXXXXObject2TempCover.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + coverXXXXObjectDir);
		MergeXXXXObject2TempCover.setConfig("outputHdfsPath", latest_tempDirCover);

		

		
		JobNode MergeXXXXObject2Temp = JobNodeModel.getJonNode4MergeXXXXObject(
				"cnipr.MergeXXXXObject2Temp",
				latest_tempDirCover,
				latestDir, 
				latest_tempDir, 
				200);
		
		

		
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject(
				"cnipr.GenNewData", 
				latest_tempDirCover, 
				latest_tempDir, 
				newXobjDir, 
				200);	
		
		
		
		
		//来自江苏所
		/*
		result.add(Cover2XXXXObject);
		Cover2XXXXObject.addChildJob(html2XXXXObject);
		html2XXXXObject.addChildJob(MergeXXXXObject2TempCover);
		MergeXXXXObject2TempCover.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(Std2DbZTCnipr);
		Std2DbZTCnipr.addChildJob(Temp2Latest);
		*/
		
		
		/////////////////////////////////////////////////////////////////////////
		//
		//
		/////////////////////////////////////////////////////////////////////////
		/**
		JobNode GenUpdateData  = new JobNode("CniprPatentParse", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.cniprpatent.GenUpdateData");
		GenUpdateData.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
		GenUpdateData.setConfig("outputHdfsPath", newXobjDir);
		 */
		
		JobNode zhuanhuan =JobNodeModel.getJobNode4Parse2XXXXObject(
				"zhuanhuan",
				DateTimeHelper.getNowTimeAsBatch(),
				"simple.jobstream.mapreduce.site.cniprpatent.zhuanhuan",
				latestDir,
				latest_tempDir,
				100);

//		result.add(zhuanhuan);
//		zhuanhuan.addChildJob(Temp2Latest);
		
		String db3DirZLF = "/RawData/cniprpatent/Stdpatentzlf";
		
		
		//上智立方/
		
		JobNode Std2DbZLFCnipr = JobNodeModel.getJobNode4Std2Db3("Std2DbZLFCnipr",
				"simple.jobstream.mapreduce.site.cniprpatent.Std2DbZLFCnipr", 
				latestDir, 
				db3DirZLF, 
				"cniprpatent",
				"/RawData/_rel_file/zlf_zl_template.db3",
				100);
		
		//result.add(Std2DbZLFCnipr);
		
		
		JobNode Std2DbZA= JobNodeModel.getJobNode4Std2Db3(
				"Std2DbZTCnipr",
				"simple.jobstream.mapreduce.common.vip.Std2Db3A", 
				latestDir, 
				db3Dir, 
				"CniprA",
				"/RawData/_rel_file/base_obj_meta_a_template_zl.db3",
				100);
//		
//		result.add(Std2DbZA);
		
		//将latest里的图片路径从cnipa 转换到cnipr 上更新
		
		
		JobNode Std2DbZTCniprall = JobNodeModel.getJobNode4Std2Db3("Std2DbZTCnipr",
				"simple.jobstream.mapreduce.site.cniprpatent.Std2DbZTCnipr", 
				latestDir, 
				db3Dir, 
				"CniprPatent",
				"/RawData/cniprpatent/template/zt_template_update.db3",
				5);
		
		result.add(Std2DbZTCniprall);
		
		return result;
	}
	
}
