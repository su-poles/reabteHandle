package com.poles.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 时间处理工具类
 */
public class DateUtils {
    private static final String PATTERN = "yyyy-MM-dd HH:mm:ss";
    private static SimpleDateFormat sdf;

    public static String format(Date date, String pattern) {
        sdf = new SimpleDateFormat(pattern);
        return sdf.format(date);
    }

    public static String format(Date date) {
        return format(date, PATTERN);
    }

    public static String addDate(Date date, int day,String pattern) {
        try {
            Calendar fromCal = Calendar.getInstance();
            fromCal.setTime(date);
            fromCal.add(Calendar.DATE, day);

            return format(fromCal.getTime(),pattern);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static String addDate(Date date, int day) {
        return addDate(date,day,PATTERN);
    }

	public static String today(Date date) {
		return format(date, PATTERN);
	}

}
