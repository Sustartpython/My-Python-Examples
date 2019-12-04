package simple.jobstream.mapreduce.common.util;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.BytesWritable;

import com.cloudera.org.codehaus.jackson.map.ser.std.IndexedStringListSerializer;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

/**
 * <p>
 * Description: 处理年月日时间相关
 * <p>
 * 
 * @author walker 2018年10月31日 上午10:45:48
 */
public class DateTimeHelper {
	/**
	 * <p>
	 * Description: 标准化日期得到八位数字
	 * <p>
	 * 
	 * @author liuqingxin 2018年10月31日 下午5:12:44
	 * @param date 需要转换包含英文的时间字符串 现在支持三种形式 日月年，月日年，月年， 中间已逗号或空格进行分割
	 * @return 20101011 8位数字的日期 如没有匹配到将返回初始的字符串
	 */
	public static String stdDate(String date) {
		String year = "";
		String month = "";
		String day = "";
		Matcher datetype1 = Pattern.compile("(\\d{1,2})[\\s,\\,]+([A-Za-z]+)[\\s,\\,]+(\\d{4})").matcher(date);
		Matcher datetype2 = Pattern.compile("([A-Za-z]+)[\\s,\\,]+(\\d{1,2})[\\s,\\,]+(\\d{4})").matcher(date);
		Matcher datetype3 = Pattern.compile("([A-Za-z]+)[\\s,\\,]+(\\d{4})").matcher(date);
		if (datetype1.find()) {
			day = datetype1.group(1);
			month = datetype1.group(2);
			year = datetype1.group(3);
		} else if (datetype2.find()) {
			day = datetype2.group(2);
			month = datetype2.group(1);
			year = datetype2.group(3);
		} else if (datetype3.find()) {
			day = "00";
			month = datetype3.group(1);
			year = datetype3.group(2);
		} else {
			return date;
		}

		if (day.length() == 1) {
			day = "0" + day;
		}
		month = month.toLowerCase();
		if (month.startsWith("jan")) {
			month = "01";
		} else if (month.startsWith("feb")) {
			month = "02";
		} else if (month.startsWith("mar")) {
			month = "03";
		} else if (month.startsWith("apr")) {
			month = "04";
		} else if (month.startsWith("may")) {
			month = "05";
		} else if (month.startsWith("jun")) {
			month = "06";
		} else if (month.startsWith("jul")) {
			month = "07";
		} else if (month.startsWith("aug")) {
			month = "08";
		} else if (month.startsWith("sep")) {
			month = "09";
		} else if (month.startsWith("oct")) {
			month = "10";
		} else if (month.startsWith("nov")) {
			month = "11";
		} else if (month.startsWith("dec")) {
			month = "12";
		} else if (month.startsWith("spring")) {
			month = "03";
		} else if (month.startsWith("summer")) {
			month = "06";
		} else if (month.startsWith("autumn")) {
			month = "09";
		} else if (month.startsWith("fall")) {
			month = "09";
		} else if (month.startsWith("winter")) {
			month = "12";
		} else {
			return year + "0000";
		}

		return year + month + day;
	}

	/**
	 * <p>
	 * Description: 将当期日期格式化为8位数字字符串
	 * </p>
	 * 
	 * @author qiuhongyang 2018年11月12日 下午3:01:46
	 * @return 返回 8 位的当期日期（20181112）
	 */
	public static String getNowDate() {
		return (new SimpleDateFormat("yyyyMMdd")).format(new Date());
	}

	/**
	 * <p>
	 * Description: 获取当前年
	 * </p>
	 * 
	 * @author qiuhongyang 2019年2月13日 上午10:38:58
	 * @return 返回 4位的当期年（2019）
	 */
	public static String getNowYear() {
		return (new SimpleDateFormat("yyyy")).format(new Date());
	}

	/**
	 * <p>
	 * Description: 检查 down_date 是否为8位数字字符串
	 * </p>
	 * 
	 * @author qiuhongyang 2018年11月27日 下午2:38:13
	 * @param downDate 下载日期
	 * @return 若是8位数字字符串，返回 true；否则，返回 false
	 */
	public static boolean checkDownDate(String downDate) {
		return Pattern.matches("\\d{8}", downDate);
	}

	/**
	 * <p>
	 * Description: 获取当前时间
	 * </p>
	 * 
	 * @author qiuhongyang 2019年4月30日 上午10:15:04
	 * @return 示例： 2019-04-30 10:15:04
	 */
	public static String getNowTime() {
		return (new SimpleDateFormat("yyyyMMdd_kkmmss")).format(new Date());
	}

	/**
	 * <p>
	 * Description: 将当期时间格式化为15位字符串作为批次号
	 * </p>
	 * 
	 * @author qiuhongyang 2018年11月12日 下午2:56:50
	 * @return 返回 15 位的 batch （20181112_145709）
	 */
	public static String getNowTimeAsBatch() {
		return (new SimpleDateFormat("yyyyMMdd_kkmmss")).format(new Date());
	}

	/**
	 * <p>
	 * Description: 检查 batch 是否为 15 为字符串（20181112_145709）
	 * </p>
	 * 
	 * @author qiuhongyang 2018年11月27日 下午2:40:13
	 * @param batch 批次号
	 * @return 若是，返回 true；否则，返回 false
	 */
	public static boolean checkBatch(String batch) {
		return Pattern.matches("\\d{8}_\\d{6}", batch);
	}

	/**
	 * <p>
	 * Description: 将当期时间格式化为15位字符串作为解析时间
	 * </p>
	 * 
	 * @author qiuhongyang 2018年11月12日 下午2:56:50
	 * @return 返回 15 位的 parse_time （20181112_145709）
	 */
	public static String getNowTimeAsParseTime() {
		return (new SimpleDateFormat("yyyyMMdd_kkmmss")).format(new Date());
	}

	/**
	 * <p>
	 * Description: 检查 parse_time 是否为 15 为字符串（20181112_145709）
	 * </p>
	 * 
	 * @author qiuhongyang 2018年11月27日 下午2:40:13
	 * @param parseTime 解析时间
	 * @return 若是，返回 true；否则，返回 false
	 */
	public static boolean checkParseTime(String parseTime) {
		return Pattern.matches("\\d{8}_\\d{6}", parseTime);
	}
	
	/**
	 * <p>
	 * Description: 按年检验范围，只适用于大于公元0年的情况
	 * </p>
	 * 
	 * @author qiuhongyang 2018年11月2日 下午4:29:19
	 * @param year      待检验的字符串
	 * @param beginYear 起始年
	 * @param endYear   结束年
	 * @return beginYear <= year <= endYear 如果符合年规范且在范围内，返回 true; 否则返回 false
	 */
	public static boolean checkYearByRange(String strYear, int beginYear, int endYear) {
		// 判断是不是非 0 打头的数字串
		if (!Pattern.matches("^[1-9]\\d*$", strYear)) {
			return false;
		}

		int intYear = Integer.parseInt(strYear);

		if (intYear < beginYear) { // 小于起始年
			return false;
		}

		if (intYear > endYear) { // 大于结束年
			return false;
		}

		return true;
	}

	/**
	 * <p>
	 * Description: 不严格按日期检验范围
	 * </p>
	 * 
	 * @author qiuhongyang 2018年11月2日 下午4:48:07
	 * @param strDate   待检验的字符串
	 * @param beginDate 起始日期
	 * @param endDate   结束日期
	 * @return beginDate <= strDate <= endDate(20180101 <= 20180202 <= 20180303)
	 *         如果符合日期规范且在范围内，返回 true; 否则返回 false
	 */
	public static boolean checkDateByRange(String strDate, int beginDate, int endDate) {
		if (strDate.length() < 1) {		// 空字符串直接返回 false
			return false;
		}

		Date dt = null;
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
		try {
			// 设置lenient为false. 否则SimpleDateFormat会比较宽松地验证日期，比如2007/02/29会被接受，并转换成2007/03/01
			format.setLenient(false);
			dt = format.parse(strDate);
		} catch (Exception ex) {
			System.err.println(strDate);
			ex.printStackTrace();
		}
		if (dt == null) {
			return false;
		}		
		int intDate = Integer.parseInt(format.format(dt));
		if (intDate < beginDate) { // 小于起始日期
			return false;
		}
		if (intDate > endDate) { // 大于结束日期
			return false;
		}
		return true;
	}

	/**
	 * <p>
	 * Description: 合并日期类动态值，有相同日期时取大值
	 * </p>
	 * 
	 * @author qiuhongyang 2019年5月5日 上午9:44:14
	 * @param limit 保留值的个数
	 * @param strings
	 * @return 按日期排序的动态值字符串
	 */
	public static String mergeDateDynamicValue(int limit, String... strings) {
		if (strings.length < 1) {
			return "";
		}

		List<String> lst = new ArrayList<String>();
		for (String items : strings) {
			for (String item : items.split(";")) {
				item = item.trim();
				if (item.length() < 1) {
					continue;
				}
				lst.add(item);
			}
		}
		
		// 按日期从旧到新排序
		Collections.sort(lst, new Comparator<String>() {
            @Override
            public int compare(String item1, String item2) {
            	String date1 = item1.split("@")[1];
            	String date2 = item2.split("@")[1];
                int i = date1.compareTo(date2);
                if ( i > 0 ) {
                    return 1;
                } 
                else if (i < 0) {
                    return -1;
                }
                else {	// 日期相同时，大值排到前面
					int cnt1 = Integer.parseInt(item1.split("@")[0]);
					int cnt2 = Integer.parseInt(item2.split("@")[0]);
					if (cnt1 > cnt2) {
						return -1;
					}
					else {
						return 1;
					}
				}
            }
        });		
		
		HashSet<String> dateSet = new HashSet<String>();
		String result = "";
		// 从旧到新累加
		for (String item : lst) {
			String date = item.split("@")[1];
			if (dateSet.contains(date)) {	// 相同日期取大值
				continue;
			}

			dateSet.add(date);
			if (dateSet.size() > limit) {
				break;
			}
			
			result += item + ";";
		}
		result = result.replaceAll(";+$", "");
		
		return result;
	}
}
