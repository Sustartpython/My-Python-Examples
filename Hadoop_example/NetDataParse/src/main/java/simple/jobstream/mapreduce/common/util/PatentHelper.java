package simple.jobstream.mapreduce.common.util;


import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.generated.regionserver.regionserver_jsp;

public class PatentHelper {
	/**
	 * @author xujiang 2019年03月25日 上午9:06:28
	 * @description 根据app_no得到校验位
	 * @param app_no 无校验位的app_no
	 * @return 校验位
	 * @throws InterruptedException 
	 */
	public static String getCheckBit4CN(String app_no) throws InterruptedException {
		app_no = app_no.replace("CN", "").replaceAll("\\.\\S$", "");
		if (app_no.length() == 12) {
			 //12位按位乘规则
			String num = "234567892345";
			String check_bit = "";
		    int allnum = 0;
		    char[] ar1 = num.toCharArray();
		    char[] ar2 = app_no.toCharArray();
		    for (int i=0;i<app_no.length();i++) {
		    	int ar1byte = Integer.parseInt(String.valueOf(ar1[i]));
		    	int ar2byte = Integer.parseInt(String.valueOf(ar2[i]));
		    	allnum = allnum + ar1byte*ar2byte;
		    }
		    int mode = allnum % 11;
		    if (mode == 10) {
		    	check_bit = "X";
		    }else {
		    	check_bit = String.valueOf(mode);
		    }
		    return check_bit;
		}else if (app_no.length() == 8) {
			String num = "23456789";
			String check_bit = "";
		    int allnum = 0;
		    char[] ar1 = num.toCharArray();
		    char[] ar2 = app_no.toCharArray();
		    for (int i=0;i<app_no.length();i++) {
		    	int ar1byte = Integer.parseInt(String.valueOf(ar1[i]));
		    	int ar2byte = Integer.parseInt(String.valueOf(ar2[i]));
		    	allnum = allnum + ar1byte*ar2byte;
		    }
		    int mode = allnum % 11;
		    if (mode == 10) {
		    	check_bit = "X";
		    }else {
		    	check_bit = String.valueOf(mode);
		    }
		    return check_bit;
		}else {
			throw new InterruptedException("传入的app_no位数不对,请检查");
		}
	}
	
	/**
	 * @author xujiang 2019年03月25日 上午9:06:28
	 * @description 根据app_no得到校验位
	 * @param app_no app_no
	 * @return 补全后的完整app_no
	 * @throws InterruptedException 
	 */
	public static String ComplementCheckBit4CN(String app_no) throws InterruptedException {
		app_no = app_no.replace("CN", "").replaceAll("\\.\\S$", "");
		String checkBit = getCheckBit4CN(app_no);
		return "CN"+app_no+"."+checkBit;
	}
	
	/**
	 * @author xujiang 2019年03月25日 上午9:36:28
	 * @description 根据app_no得到专利类型
	 * @param app_no 无校验位的app_no
	 * @return 转换后的字符串 1d3g2
	 * @throws InterruptedException 
	 */
	public static String getTypeBit4CN(String app_no) throws InterruptedException{
		app_no = app_no.replace("CN", "").replaceAll("\\.\\S$", "");
		String typeStrings = "";
		if (app_no.length() == 12) {
		    char[] ar2 = app_no.toCharArray();
		    typeStrings = String.valueOf(ar2[4]);
		    return typeStrings;
		}else if (app_no.length() == 8) {
			 char[] ar2 = app_no.toCharArray();
			 typeStrings = String.valueOf(ar2[2]);
			 return typeStrings;
		}else {
			throw new InterruptedException("传入的app_no位数不对,请检查");
		}
	}
	
	/**
	 * @author xujiang 2019年03月25日 上午9:43:28
	 * @description 根据传入app_no判断校验位是否正确
	 * @param app_no app_no
	 * @return true false
	 */
	public static boolean isCorrectCheckBit4CN(String app_no){
		if (app_no.length() == 12) {
			if ("CN".equals(app_no.substring(0, 2))){
				try {
					String results= ComplementCheckBit4CN(app_no);
					if (app_no.equals(results)){
						return true;
					}else {
						return false;
					}
				} catch (Exception e) {
					// TODO: handle exception
					return false;
				}
			}else {
				return false;
			}
		}else if (app_no.length() == 16) {
			if ("CN".equals(app_no.substring(0, 2))){
				try {
					String results= ComplementCheckBit4CN(app_no);
					if (app_no.equals(results)){
						return true;
					}else {
						return false;
					}
				} catch (Exception e) {
					// TODO: handle exception
					return false;
				}
				
			}else {
				return false;
			}
		}else {
			//throw new InterruptedException("传入的app_no位数不对,请检查");
			return false;
		}
	}
	
	
	
	
	
	
	
	
	

	
}
