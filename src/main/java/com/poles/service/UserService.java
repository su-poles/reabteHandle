package com.poles.service;

import java.sql.Connection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.poles.utils.BeanMapUtils;
import com.poles.utils.DateUtils;

public class UserService {
	public List<Map<String, String>> getUserListBefore(Connection conn, Date date) {
//        String strDate = DateUtils.addDate(date, -90);
//        String querySQL = "select id, rebateSum from t_user where createTime <= '" + strDate + "' and rebateSum > 0";
        String querySQL = "select id, rebateSum from t_user";
        return BeanMapUtils.querySQLToMap(conn, querySQL);
	}
}
