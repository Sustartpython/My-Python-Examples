package simple.jobstream.mapreduce.common.vip;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class LogMR {
	/**
	 * <p>Description: 记录日志到HDFS</p>  
	 * @author qiuhongyang 2018年11月18日 下午4:05:46
	 * @param context 上下文
	 * @param pathfile HDFS文件绝对路径
	 * @param text 需要记录的日志信息
	 */
	public static void log2HDFS4Mapper(@SuppressWarnings("rawtypes") org.apache.hadoop.mapreduce.Mapper.Context context, String pathfile, String text) {
		Date dt = new Date();// 如果不需要格式,可直接用dt,dt就是当前系统时间
		DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");// 设置显示格式
		String nowTime = df.format(dt);
//		df = new SimpleDateFormat("yyyyMMdd");// 设置显示格式
//		String nowDate = df.format(dt);

		text = nowTime + "\n" + text + "\n\n";

		boolean bException = false;
		BufferedWriter out = null;
		try {
			// 获取HDFS文件系统
			FileSystem fs = FileSystem.get(context.getConfiguration());

			FSDataOutputStream fout = null;
//			String pathfile = "/user/qhy/log/log_map/" + nowDate + ".txt";
			if (fs.exists(new Path(pathfile))) {
				fout = fs.append(new Path(pathfile));
			} else {
				fout = fs.create(new Path(pathfile));
			}

			out = new BufferedWriter(new OutputStreamWriter(fout, "UTF-8"));
			out.write(text);
			out.close();

		} catch (Exception ex) {
			bException = true;
			ex.printStackTrace();
		}

		if (bException) {
			// TODO
		} else {
			// TODO
		}
	}
	
	/**
	 * <p>Description: 记录日志到HDFS</p>  
	 * @author qiuhongyang 2018年11月18日 下午4:05:46
	 * @param context 上下文
	 * @param pathfile HDFS文件绝对路径
	 * @param text 需要记录的日志信息
	 */
	public static void log2HDFS4Reducer(@SuppressWarnings("rawtypes") org.apache.hadoop.mapreduce.Reducer.Context context, String pathfile, String text) {
		Date dt = new Date();// 如果不需要格式,可直接用dt,dt就是当前系统时间
		DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");// 设置显示格式
		String nowTime = df.format(dt);
//		df = new SimpleDateFormat("yyyyMMdd");// 设置显示格式
//		String nowDate = df.format(dt);

		text = nowTime + "\n" + text + "\n\n";

		boolean bException = false;
		BufferedWriter out = null;
		try {
			// 获取HDFS文件系统
			FileSystem fs = FileSystem.get(context.getConfiguration());

			FSDataOutputStream fout = null;
//			String pathfile = "/user/qhy/log/log_map/" + nowDate + ".txt";
			if (fs.exists(new Path(pathfile))) {
				fout = fs.append(new Path(pathfile));
			} else {
				fout = fs.create(new Path(pathfile));
			}

			out = new BufferedWriter(new OutputStreamWriter(fout, "UTF-8"));
			out.write(text);
			out.close();

		} catch (Exception ex) {
			bException = true;
			ex.printStackTrace();
		}

		if (bException) {
			// TODO
		} else {
			// TODO
		}
	}
}
