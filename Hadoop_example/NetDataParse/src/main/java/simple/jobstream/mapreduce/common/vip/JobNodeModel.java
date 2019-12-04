package simple.jobstream.mapreduce.common.vip;

import com.vipcloud.JobNode.JobNode;

/**
 * <p>
 * Description: 几种典型的 JobNode
 * <p>
 * 
 * @author walker 2018年10月31日 上午10:45:48
 */
public class JobNodeModel {
	/**
	 * <p>
	 * Description: 拷贝 XXXXObject 到另一个目录
	 * </p>
	 * 
	 * @author qiuhongyang 2018年10月31日 上午11:21:49
	 * @param jobName        任务名称
	 * @param inputHdfsPath  源目录（被拷贝目录）
	 * @param outputHdfsPath 目标目录（拷贝输出目录）
	 * @return JobNode
	 */
	public static JobNode getJobNode4CopyXXXXObject(String jobName, String inputHdfsPath, String outputHdfsPath) {

		JobNode jobNode = new JobNode(jobName, "", 0, "simple.jobstream.mapreduce.common.vip.CopyXXXXObject");
		jobNode.setConfig("jobName", jobName);
		jobNode.setConfig("inputHdfsPath", inputHdfsPath);
		jobNode.setConfig("outputHdfsPath", outputHdfsPath);

		return jobNode;
	}

	/**
	 * <p>
	 * Description: 合并两个目录的 XXXXObject，按字段融合相同ID的数据， 根据 XXXXObject 中的 down_date 和
	 * parse_time 选出主记录（major） 和补充记录（minor），当主记录字段缺失时用补充记录的字段补充。 down_date 的格式应为
	 * 20181105，parse_time 的格式为 20181105_080808
	 * </p>
	 * 
	 * @author qiuhongyang 2018年11月5日 上午9:53:57
	 * @param jobName        任务名称
	 * @param inPathX        输入路径1
	 * @param inPathY        输入路径2
	 * @param outputHdfsPath 输出路径
	 * @param reduceNum      reduce 个数
	 * @return JobNode
	 */
	public static JobNode getJonNode4MergeXXXXObject(String jobName, String inPathX, String inPathY,
			String outputHdfsPath, int reduceNum) {
		JobNode jobNode = new JobNode(jobName, "", 0, "simple.jobstream.mapreduce.common.vip.MergeXXXXObject");
		jobNode.setConfig("jobName", jobName);
		jobNode.setConfig("inPathX", inPathX);
		jobNode.setConfig("inPathY", inPathY);
		jobNode.setConfig("reduceNum", String.valueOf(reduceNum));
		jobNode.setConfig("inputHdfsPath", inPathX + "," + inPathY);
		jobNode.setConfig("outputHdfsPath", outputHdfsPath);

		return jobNode;
	}
	
	/**
	 * <p>
	 * Description: 合并两个目录的 XXXXObject，按字段融合相同ID的数据， 根据 XXXXObject 中的 down_date 和
	 * parse_time 选出主记录（major） 和补充记录（minor），当主记录字段缺失时用补充记录的字段补充。 down_date 的格式应为
	 * 20181105，parse_time 的格式为 20181105_080808。
	 * 会合并 down_cnt 和 cited_cnt
	 * </p>
	 * 
	 * @author qiuhongyang 2019年5月7日 上午9:53:57
	 * @param jobName        任务名称
	 * @param inPathX        输入路径1
	 * @param inPathY        输入路径2
	 * @param outputHdfsPath 输出路径
	 * @param reduceNum      reduce 个数
	 * @return JobNode
	 */
	public static JobNode getJonNode4MergeXXXXObjectMergeDateDynamicValue(String jobName, String inPathX, String inPathY,
			String outputHdfsPath, int reduceNum) {
		JobNode jobNode = new JobNode(jobName, "", 0, "simple.jobstream.mapreduce.common.vip.MergeXXXXObjectMergeDateDynamicValue");
		jobNode.setConfig("jobName", jobName);
		jobNode.setConfig("inPathX", inPathX);
		jobNode.setConfig("inPathY", inPathY);
		jobNode.setConfig("reduceNum", String.valueOf(reduceNum));
		jobNode.setConfig("inputHdfsPath", inPathX + "," + inPathY);
		jobNode.setConfig("outputHdfsPath", outputHdfsPath);

		return jobNode;
	}
	
	
	
	/**
	 * <p>Description: 根据 parse_time 选最新一组引文</p>  
	  * @author qiuhongyang 2018年11月5日 上午9:53:57
	 * @param jobName        任务名称
	 * @param inPathX        输入路径1
	 * @param inPathY        输入路径2
	 * @param outputHdfsPath 输出路径
	 * @param reduceNum      reduce 个数
	 * @return JobNode
	 */
	public static JobNode getJonNode4MergeXXXXObject4Ref(String jobName, String inPathX, String inPathY, String outputHdfsPath, int reduceNum) {
		JobNode jobNode = new JobNode(jobName, "", 0, "simple.jobstream.mapreduce.common.vip.MergeXXXXObject4Ref");
		jobNode.setConfig("jobName", jobName);
		jobNode.setConfig("inPathX", inPathX);
		jobNode.setConfig("inPathY", inPathY);
		jobNode.setConfig("reduceNum", String.valueOf(reduceNum));
		jobNode.setConfig("inputHdfsPath", inPathX + "," + inPathY);
		jobNode.setConfig("outputHdfsPath", outputHdfsPath);

		return jobNode;
	}

	/**
	 * <p>
	 * Description: 用 inPathKey 中的 key 提取 inPathY 中的数据。 
	 * 结果数据的 key 为 inPathX 和 inPathY 的交集， 结果数据的 XXXXObject 为 inPathY 中的记录。
	 * 可用于题录提取题录或引文提取题录。
	 * </p>
	 * 
	 * @author qiuhongyang 2018年11月5日 上午10:58:52
	 * @param jobName        任务名称
	 * @param inPathKey        提供 key 的路径
	 * @param inPathY        提供 XXXXObject 的路径
	 * @param outputHdfsPath 输出路径
	 * @param reduceNum      reduce 个数
	 * @return JobNode
	 */
	public static JobNode getJonNode4ExtractXXXXObject(String jobName, String inPathKey, String inPathY,
			String outputHdfsPath, int reduceNum) {
		JobNode jobNode = new JobNode(jobName, "", 0, "simple.jobstream.mapreduce.common.vip.ExtractXXXXObject");
		jobNode.setConfig("jobName", jobName);
		jobNode.setConfig("inPathX", inPathKey);
		jobNode.setConfig("inPathY", inPathY);
		jobNode.setConfig("reduceNum", String.valueOf(reduceNum));
		jobNode.setConfig("inputHdfsPath", inPathKey + "," + inPathY);
		jobNode.setConfig("outputHdfsPath", outputHdfsPath);

		return jobNode;
	}
	
	/**
	 * <p>
	 * Description: 用 inPathKey 中的 key 提取 inPathVal 中的引文数据。 
	 * 结果数据的 key 为 inPathKey 和 inPathVal 的交集，
	 * 结果数据的 XXXXObject 为 inPathVal 中的记录。一个key对应多条引文。
	 * 可用于题录提取引文或引文提取引文。
	 * </p>
	 * 
	 * @author qiuhongyang 2019年2月19日 下午1:50:17
	 * @param jobName        任务名称
	 * @param inPathKey        提供 key 的路径
	 * @param inPathVal        提供 XXXXObject 的路径
	 * @param outputHdfsPath 输出路径
	 * @param reduceNum      reduce 个数
	 * @return JobNode
	 */
	public static JobNode getJonNode4ExtractXXXXObject4Ref(String jobName, String inPathKey, String inPathVal,
			String outputHdfsPath, int reduceNum) {
		JobNode jobNode = new JobNode(jobName, "", 0, "simple.jobstream.mapreduce.common.vip.ExtractXXXXObject4Ref");
		jobNode.setConfig("jobName", jobName);
		jobNode.setConfig("inPathKey", inPathKey);
		jobNode.setConfig("inPathVal", inPathVal);
		jobNode.setConfig("reduceNum", String.valueOf(reduceNum));
		jobNode.setConfig("inputHdfsPath", inPathKey + "," + inPathVal);
		jobNode.setConfig("outputHdfsPath", outputHdfsPath);

		return jobNode;
	}

	/**
	 * <p>
	 * Description: 提取 XXXXObject 的数据到db3
	 * </p>
	 * 
	 * @author liuqingxin 2018年11月26日 下午14:40:30
	 * @param jobName        任务名称
	 * @param className      需要执行的类名
	 * @param inputHdfsPath  源目录（提供 XXXXObject 的路径）
	 * @param outputHdfsPath 目标目录（输出目录）
	 * @param postfixDb3     db3文件名
	 * @param tempFileDb3    db3模版文件路径
	 * @param reduceNum      reduce 个数
	 * @return JobNode
	 */
	public static JobNode getJobNode4Std2Db3(String jobName, String className, String inputHdfsPath,
			String outputHdfsPath, String postfixDb3, String tempFileDb3, int reduceNum) {

		JobNode jobNode = new JobNode(jobName, "", 0, className);
		jobNode.setConfig("jobName", jobName);
		jobNode.setConfig("inputHdfsPath", inputHdfsPath);
		jobNode.setConfig("outputHdfsPath", outputHdfsPath);
		jobNode.setConfig("postfixDb3", postfixDb3);
		jobNode.setConfig("tempFileDb3", tempFileDb3);
		jobNode.setConfig("reduceNum", String.valueOf(reduceNum));

		return jobNode;
	}

	/**
	 * <p>
	 * Description: 解析 html 或 json 到 XXXXObject
	 * </p>
	 * 
	 * @author qiuhongyang 2018年11月27日 下午2:54:28
	 * @param jobName        任务名称
	 * @param batch          批次号，格式同解析时间（20181212_101010）
	 * @param className      需要执行的类名
	 * @param inputHdfsPath  源目录（big_json 目录）
	 * @param outputHdfsPath 目标目录（输出目录）
	 * @param reduceNum      reduce 个数
	 * @return
	 */
	public static JobNode getJobNode4Parse2XXXXObject(String jobName, String batch, String className, 
			String inputHdfsPath, String outputHdfsPath, int reduceNum) {
		JobNode jobNode = new JobNode(jobName, "", 0, className);
		jobNode.setConfig("jobName", jobName);
		jobNode.setConfig("batch", batch);
		jobNode.setConfig("inputHdfsPath", inputHdfsPath);
		jobNode.setConfig("outputHdfsPath", outputHdfsPath);
		jobNode.setConfig("reduceNum", String.valueOf(reduceNum));

		return jobNode;
	}
	
	/**
	 * <p>
	 * Description: 解析引文数据到 XXXXObject
	 * </p>
	 * 
	 * @author qiuhongyang 2018年11月27日 下午2:54:28
	 * @param jobName        任务名称
	 * @param batch          批次号，格式同解析时间（20181212_101010）
	 * @param className      需要执行的类名
	 * @param inputHdfsPath  源目录（big_json 目录）
	 * @param outputHdfsPath 目标目录（输出目录）
	 * @param reduceNum      reduce 个数
	 * @return
	 */
	public static JobNode getJobNode4ParseRef2XXXXObject(String jobName, String batch, String className, 
			String inputHdfsPath, String outputHdfsPath, int reduceNum) {
		JobNode jobNode = new JobNode(jobName, "", 0, className);
		jobNode.setConfig("jobName", jobName);
		jobNode.setConfig("batch", batch);
		jobNode.setConfig("inputHdfsPath", inputHdfsPath);
		jobNode.setConfig("outputHdfsPath", outputHdfsPath);
		jobNode.setConfig("reduceNum", String.valueOf(reduceNum));

		return jobNode;
	}

	/**
	 * <p>
	 * Description: 移动目录，如果目标路径存在会被删除
	 * </p>
	 * 
	 * @author qiuhongyang 2018年12月11日 上午11:28:23
	 * @param srcDir 源目录
	 * @param dstDir 目标目录
	 * @return
	 */
	public static JobNode getJobNode4MoveDir(String srcDir, String dstDir) {
		JobNode jobNode = new JobNode("SimpleJob", "", 0, "simple.jobstream.mapreduce.common.vip.MoveDir");
		jobNode.setConfig("srcDir", srcDir);
		jobNode.setConfig("dstDir", dstDir);

		return jobNode;
	}

	/**
	 * <p>
	 * Description: 将 db3 数据转成 XXXXObject 导入到 hdfs
	 * </p>
	 * 
	 * @author qiuhongyang 2018年12月13日 上午9:45:44
	 * @param jobName        任务名称
	 * @param tableName      db3数据库表名
	 * @param keyField       主键字段名
	 * @param inputHdfsPath  源目录（db3 目录）
	 * @param outputHdfsPath 目标目录（XXXXObject 目录）
	 * @return
	 */
	public static JobNode getJobNode4Sqlite2XXXXObject(String jobName, String tableName, String keyField,
			String inputHdfsPath, String outputHdfsPath) {
		JobNode jobNode = new JobNode(jobName, "", 0, "simple.jobstream.mapreduce.common.vip.Sqlite2XXXXObject");

		jobNode.setConfig("jobName", jobName);
		jobNode.setConfig("inputHdfsPath", inputHdfsPath);
		jobNode.setConfig("outputHdfsPath", outputHdfsPath);
		jobNode.setConfig("tableName", tableName); // 表名
		jobNode.setConfig("keyField", keyField); // 主键字段名

		return jobNode;
	}

	/**
	 * <p>
	 * Description: 将 XXXXObject 转为 HFile 文件
	 * </p>
	 * 
	 * @author qiuhongyang 2018年12月13日 下午2:21:10
	 * @param jobName        任务名称
	 * @param inputHdfsPath  源目录（XXXXObject 目录）
	 * @param outputHdfsPath 目标目录（HFile 目录）
	 * @param hbaseTableName HBase表名
	 * @param familyName     列簇
	 * @param HRegionCount   Region数量
	 * @param IsClear        是否清空原表
	 * @return
	 */
	public static JobNode getJobNode4XXXXObject2HFile(String jobName, String inputHdfsPath, String outputHdfsPath,
			String hbaseTableName, String familyName, int HRegionCount, boolean IsClear) {
		JobNode jobNode = new JobNode(jobName, "", 0, "simple.jobstream.mapreduce.common.vip.XXXXObject2HFile");

		jobNode.setConfig("jobName", jobName);
		jobNode.setConfig("inputHdfsPath", inputHdfsPath);
		jobNode.setConfig("outputHdfsPath", outputHdfsPath);
		jobNode.setConfig("tableName", hbaseTableName);
		jobNode.setConfig("familyName", familyName);
		jobNode.setConfig("HRegionCount", Integer.toString(HRegionCount));
		if (IsClear) {
			jobNode.setConfig("IsClear", "true");
		} else {
			jobNode.setConfig("IsClear", "false");
		}

		return jobNode;
	}

	/**
	 * <p>Description: 加载 HFile 到 HBase </p>  
	 * @author qiuhongyang 2018年12月13日 下午3:18:51
	 * @param jobName 任务名称
	 * @param hfilePath HFile目录
	 * @param hbaseTableName  HBase表名
	 * @param familyName 列簇
	 * @return
	 */
	public static JobNode getJobNode4LoadHFile(String jobName, String hfilePath, 
			String hbaseTableName, String familyName) {
		JobNode jobNode = new JobNode(jobName, "", 0, "simple.jobstream.mapreduce.common.vip.LoadHFile");

		jobNode.setConfig("jobName", jobName);
		jobNode.setConfig("hfilePath", hfilePath);
		jobNode.setConfig("tableName", hbaseTableName);
		jobNode.setConfig("familyName", familyName);

		return jobNode;
	}
	
	
	/**
	 * <p>
	 * Description: 将ref_id和cited_id写回 XXXXObject      主题录不存在时，不会输出XXXXObject
	 * </p>
	 * 
	 * @author liuqingxin 2019年01月21日 下午14:20:30
	 * @param jobName        任务名称
	 * @param inPathRefid        提供 refid 的路径 如果不需要该目录填空字符串
	 * @param inPathCitedid        提供 citedid 的路径 如果不需要该目录填空字符串
	 * @param inPathXXXXObject        提供 XXXXObject 的路径
	 * @param outputHdfsPath 输出路径
	 * @param reduceNum      reduce 个数
	 * @return JobNode
	 */
	public static JobNode getJobNode4MergeRefidCitedid(String jobName, String inPathRefid, String inPathCitedid,
			String inPathXXXXObject, String outputHdfsPath, int reduceNum) {
//		System.out.println("***inPathRefid:" + inPathRefid);
//		System.out.println("***inPathCitedid:" + inPathCitedid);
//		System.out.println("***inPathXXXXObject:" + inPathXXXXObject);
		
		JobNode jobNode = new JobNode(jobName, "", 0, "simple.jobstream.mapreduce.common.vip.MergeRefidCitedid");
		jobNode.setConfig("jobName", jobName);
		jobNode.setConfig("inPathRefid", inPathRefid);
		jobNode.setConfig("inPathCitedid", inPathCitedid);
		jobNode.setConfig("inPathXXXXObject", inPathXXXXObject);
		jobNode.setConfig("reduceNum", String.valueOf(reduceNum));
		if (inPathRefid.equals("")) {
			jobNode.setConfig("inputHdfsPath", inPathCitedid + "," + inPathXXXXObject);
		}
		else if (inPathCitedid.equals("")) {
			jobNode.setConfig("inputHdfsPath", inPathRefid + "," + inPathXXXXObject);
		}
		else {
			jobNode.setConfig("inputHdfsPath", inPathRefid + "," + inPathCitedid + "," + inPathXXXXObject);
		}		
		jobNode.setConfig("outputHdfsPath", outputHdfsPath);

		return jobNode;
	}
	
	
	/**
	 * <p>
	 * Description: 将ref_cnt和cited_cnt写回 XXXXObject      主题录不存在时，不会输出XXXXObject
	 * </p>
	 * 
	 * @author liuqingxin 2019年02月14日 下午17:21:30
	 * @param jobName        任务名称
	 * @param inPathRefcnt        提供 refcnt 的路径 如果不需要该目录填空字符串
	 * @param inPathCitedcnt       提供 citedcnt 的路径 如果不需要该目录填空字符串
	 * @param inPathXXXXObject        提供 XXXXObject 的路径
	 * @param outputHdfsPath 输出路径
	 * @param reduceNum      reduce 个数
	 * @return JobNode
	 */
	public static JobNode getJobNode4MergeRefcntCitedcnt(String jobName, String inPathRefcnt, String inPathCitedcnt,
			String inPathXXXXObject, String outputHdfsPath, int reduceNum) {
		JobNode jobNode = new JobNode(jobName, "", 0, "simple.jobstream.mapreduce.common.vip.MergeRefcntCitedcnt");
		jobNode.setConfig("jobName", jobName);
		jobNode.setConfig("inPathRefcnt", inPathRefcnt);
		jobNode.setConfig("inPathCitedcnt", inPathCitedcnt);
		jobNode.setConfig("inPathXXXXObject", inPathXXXXObject);
		jobNode.setConfig("reduceNum", String.valueOf(reduceNum));
		if (inPathRefcnt.equals("")) {
			jobNode.setConfig("inputHdfsPath", inPathCitedcnt + "," + inPathXXXXObject);
		}		
		else if (inPathCitedcnt.equals("") ) {
			jobNode.setConfig("inputHdfsPath", inPathRefcnt + "," + inPathXXXXObject);
		}
		//ref和cited文件夹相同时，只传入ref文件夹
		else if (inPathRefcnt.equals(inPathCitedcnt)){
			jobNode.setConfig("inputHdfsPath", inPathRefcnt + "," + inPathXXXXObject);
		}
		else {
			jobNode.setConfig("inputHdfsPath", inPathRefcnt + "," + inPathCitedcnt + "," + inPathXXXXObject);
		}			
				
		jobNode.setConfig("outputHdfsPath", outputHdfsPath);

		return jobNode;
	}
}
