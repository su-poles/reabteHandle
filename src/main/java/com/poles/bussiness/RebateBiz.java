package com.poles.bussiness;

import java.math.BigDecimal;
import java.sql.Connection;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.jcraft.jsch.Session;
import com.poles.jsch.DBUtil;
import com.poles.service.UserService;
import com.poles.utils.BeanMapUtils;
import com.poles.utils.DateUtils;

public class RebateBiz {
	public void process5(){
		int[] userIds = new int[]{18890454, 18890456, 18890457, 18890459, 18890461, 18892135, 18892155, 18898211, 18899907, 18899908, 18900153, 18900169, 18900297, 18900314, 18900336, 18900516, 18900692, 18900714, 18901035, 18901301, 18902184, 18904171, 18907158, 18907981, 18909899, 18911232, 18911362, 18967273, 18972990, 18998037, 19008255, 19009161, 19010506, 19010927, 19011561, 19011570, 19011675, 19013196, 19019892, 19021378, 19022382, 19022546, 19023930, 19024419, 19025012, 19025210, 19025608, 19026573, 19026755, 19026939, 19027044, 19028083, 19028087, 19028105, 19028888, 19029618, 19030636, 19030725, 19031217, 19031312, 19031466, 19031614, 19031865, 19032314, 19032751, 19032987, 19033795, 19033862, 19034003, 19034109, 19034191, 19034211, 19034273, 19034311, 19034808, 19034840, 19034870, 19034954, 19035034, 19035522, 19035537, 19035819, 19036133, 19036296, 19036398, 19037909, 19038777, 19039278, 19039279, 19039280, 19039281, 19039282, 19039284};
		Calendar c = Calendar.getInstance();
		c.set(2016, 5, 15, 15, 10, 00); // 2016-06-15 13:00:00
		System.out.println(c.getTime());
		Session session = null;
        Connection conn = null;
        try{
        	session = DBUtil.openssh();
        	conn = DBUtil.getConnection();
			String strDate = DateUtils.addDate(c.getTime(), -90);
			
			for(int userId : userIds){
				String sql = "select rebateSum from t_user where id = " + userId;
				BigDecimal 用户目前返利余额 = BeanMapUtils.queryReabte(conn, sql);
				
//				String queryRebateSum = "select IFNULL(sum(handleSum),0) as rebateSum from t_fundrecord where userId = " + userId + " and operateType in (800,801,802,803,804,806,820) and recordTime <= '"+strDate+"'"; 
				String queryRebateSum = "select IFNULL(sum(handleSum),0) as rebateSum from t_fundrecord where userId = " + userId + " and operateType in (800,801,802,803,804,806,820)"; 
				BigDecimal 三个月前发放总返利 = BeanMapUtils.queryReabte(conn, queryRebateSum);
				
				String queryKC = "select IFNULL(sum(handleSum),0) as kcRebate from t_fundrecord where userId = " + userId + " and operateType in (812, 814)";
				BigDecimal 扣除返利 = BeanMapUtils.queryReabte(conn, queryKC);
				
				//已投资返利：查询用户此前所有使用返利的投资记录
				String queryInvest = "select sum(t.investRebate) as investRebate from (select IFNULL(sum(fanliAmount),0) as investRebate from t_invest where investor = " + userId + " union all select IFNULL(sum(handleSum),0) as investRebate from t_fundrecord where operateType = 811  and userId = "+userId+") t ";
				BigDecimal 投资返利 = BeanMapUtils.queryReabte(conn, queryInvest);
				
				if(三个月前发放总返利.subtract(扣除返利).subtract(投资返利).compareTo(用户目前返利余额) != 0){
//					System.out.println("【userId: "+userId+"】, 用户目前返利余额："+用户目前返利余额+", 三个月前发放总返利："+ 三个月前发放总返利 + ", 已扣除：" + 扣除返利 + ", 总投资："+投资返利+"(应该扣除："+(三个月前发放总返利.subtract(扣除返利).subtract(投资返利))+")");
					System.out.println("【userId: "+userId+"】, 用户目前返利余额："+用户目前返利余额+", 发放总返利："+ 三个月前发放总返利 + ", 已扣除：" + 扣除返利 + ", 总投资："+投资返利);
//					BigDecimal handleSum = 三个月前发放总返利.subtract(扣除返利).subtract(投资返利);
//					System.out.println("INSERT INTO `duomeidai`.`t_fundrecord` (`userId`, `fundMode`, `handleSum`, `usableSum`, `freezeSum`, `dueinSum`, `trader`, `recordTime`, `dueoutSum`, `remarks`, `operateType`, `oninvest`, `paynumber`, `paybank`, `cost`, `income`, `spending`, `borrow_id`, `repayment_id`, `planId`) VALUES ('"+userId+"', '到期未使用扣除返利', '"+handleSum+"', '0.00', '0.00', '0.00', '"+userId+"', '2016-09-14 15:10:07', '0.00', '平帐，到期未使用返利收回，金额："+handleSum+"', '812', '0.00', NULL, NULL, '0.00', '0.00', '"+handleSum+"', '-1', '-1', '-1');");
				}
				
				
			}			
        }catch (Exception e){
        	e.printStackTrace();
        }finally{
        	DBUtil.close(conn);
        	DBUtil.disConnection(session);
        }
		
	}
	
	public void process4(){
		Calendar c = Calendar.getInstance();
		c.set(2016, 8, 13, 13, 00, 00); // 2016-08-09 13:00:00
		
		Session session = null;
        Connection conn = null;
        try{
        	session = DBUtil.openssh();
        	conn = DBUtil.getConnection();
			
			String userId = "18900314";
			String 用户目前返利余额 = "34.08";
			
			//总返利
			String queryRebateSum = "select IFNULL(sum(handleSum),0) as rebateSum from t_fundrecord where userId = " + userId + " and operateType in (800,801,802,803,804,806,820)"; 
			BigDecimal 发放返利 = BeanMapUtils.queryReabte(conn, queryRebateSum);
			
			//已扣除返利：查询用户所有扣除的返利        返利扣除（812, 814）
			String queryKC = "select IFNULL(sum(handleSum),0) as kcRebate from t_fundrecord where userId = " + userId + " and operateType in (812, 814)";
			BigDecimal 扣除返利 = BeanMapUtils.queryReabte(conn, queryKC);
			
			//已投资返利：查询用户此前所有使用返利的投资记录
			String queryInvest = "select sum(t.investRebate) from (select IFNULL(sum(fanliAmount),0) as investRebate from t_invest where investor = " + userId + " union all select IFNULL(sum(handleSum),0) as investRebate from t_fundrecord where operateType = 811  and userId = "+userId+") t ";
			BigDecimal 投资返利 = BeanMapUtils.queryReabte(conn, queryInvest);
			
			System.out.println("【" + "userId: "+userId+"】 用户返利余额："+用户目前返利余额+", 总发放返利："+ 发放返利 + ", 已扣除：" + 扣除返利 + ", 总投资："+投资返利);
        }catch (Exception e){
        	e.printStackTrace();
        }finally{
        	DBUtil.close(conn);
        	DBUtil.disConnection(session);
        }
	}
	
	public void process3(){
		Calendar c = Calendar.getInstance();
		c.set(2016, 8, 13, 13, 00, 00); // 2016-08-09 13:00:00
		
		Session session = null;
        Connection conn = null;
        try{
        	session = DBUtil.openssh();
        	conn = DBUtil.getConnection();
			//查询3个月（90天）前创建的用户Id
			UserService us = new UserService();
			List<Map<String, String>> userList = us.getUserListBefore(conn, c.getTime());
			
			//当天
			String strToday = DateUtils.today(c.getTime());
			
			for(Map<String, String> userMap : userList){
				String userId = userMap.get("id");
				String 用户目前返利余额 = userMap.get("rebateSum");
				
				//总返利
				String queryRebateSum = "select IFNULL(sum(handleSum),0) as rebateSum from t_fundrecord where userId = " + userId + " and operateType in (800,801,802,803,804,806,820)"; 
				BigDecimal 发放返利 = BeanMapUtils.queryReabte(conn, queryRebateSum);
				
				//已扣除返利：查询用户所有扣除的返利        返利扣除（812, 814）
				String queryKC = "select IFNULL(sum(handleSum),0) as kcRebate from t_fundrecord where userId = " + userId + " and operateType in (812, 814)";
				BigDecimal 扣除返利 = BeanMapUtils.queryReabte(conn, queryKC);
				
				//已投资返利：查询用户此前所有使用返利的投资记录
				String queryInvest = "select sum(t.investRebate) from (select IFNULL(sum(fanliAmount),0) as investRebate from t_invest where investor = " + userId + " union all select IFNULL(sum(handleSum),0) as investRebate from t_fundrecord where operateType = 811  and userId = "+userId+") t ";
				BigDecimal 投资返利 = BeanMapUtils.queryReabte(conn, queryInvest);
				
				//发放返利 - 扣除返利 - 投资返利 == 用户目前返利余额
				if(new BigDecimal(用户目前返利余额).compareTo(发放返利.subtract(扣除返利).subtract(投资返利)) != 0){					
					System.out.println("【" + strToday+"】userId: "+userId+", 用户返利余额："+用户目前返利余额+", 总发放返利："+ 发放返利 + ", 已扣除：" + 扣除返利 + ", 总投资："+投资返利);
				}
			}			
        }catch (Exception e){
        	e.printStackTrace();
        }finally{
        	DBUtil.close(conn);
        	DBUtil.disConnection(session);
        }
	}
	
	public void process2(){
		Calendar c = Calendar.getInstance();
		c.set(2016, 8, 13, 13, 00, 00); // 2016-08-09 13:00:00
		
		Session session = null;
        Connection conn = null;
        try{
        	session = DBUtil.openssh();
        	conn = DBUtil.getConnection();
			//查询3个月（90天）前创建的用户Id
			UserService us = new UserService();
			List<Map<String, String>> userList = us.getUserListBefore(conn, c.getTime());
			
			//当天
			String strToday = DateUtils.today(c.getTime());
			//当天前90
			String strDate = DateUtils.addDate(c.getTime(), -90);
			
			for(Map<String, String> userMap : userList){
				String userId = userMap.get("id");
				String rebateNum = userMap.get("rebateSum");
				
				//总返利
				String queryRebateSum = "select IFNULL(sum(handleSum),0) as rebateSum from t_fundrecord where userId = " + userId + " and operateType in (800,801,802,803,804,806,820)"; 
				BigDecimal rebateSum = BeanMapUtils.queryReabte(conn, queryRebateSum);
				
				//三个月前总返利： 查询用户3个月之前获取的返利    返利发放（800,801,802,803,804,806,820）
				String queryTotale = "select IFNULL(sum(handleSum),0) as totalRebate from t_fundrecord where userId = " + userId + " and operateType in (800,801,802,803,804,806,820) and "
						+ "recordTime <= '"+strDate + "'";
				BigDecimal totalRebate = BeanMapUtils.queryReabte(conn, queryTotale);
				
				//已扣除返利：查询用户所有扣除的返利        返利扣除（811, 812, 814）
				String queryKC = "select IFNULL(sum(handleSum),0) as kcRebate from t_fundrecord where userId = " + userId + " and operateType in (812, 814) and recordTime <= '"+strToday+"'";
				BigDecimal kcRebate = BeanMapUtils.queryReabte(conn, queryKC);
				
				//已投资返利：查询用户此前所有使用返利的投资记录
				String queryInvest = "select sum(t.investRebate) from (select IFNULL(sum(fanliAmount),0) as investRebate from t_invest where investTime <= '"+strToday+"' and investor = " + userId + " union all select IFNULL(sum(handleSum),0) as investRebate from t_fundrecord where operateType = 811  and userId = "+userId+" and recordTime <= '"+strDate+"') t ";
				BigDecimal investRebate = BeanMapUtils.queryReabte(conn, queryInvest);
				
				//rebateSum - (totalRebate - kcRebate - investRebate)
				BigDecimal rest = new BigDecimal(rebateNum).subtract((totalRebate.subtract(kcRebate).subtract(investRebate)));
				if(new BigDecimal(rebateNum).compareTo(rest) != 0 && investRebate.compareTo(totalRebate) == -1){					
					System.out.println("【" + strToday+"】userId: "+userId+", 用户返利余额："+rebateNum+", 90天前总发放返利："+ rebateSum + ", 应该剩余返利：" + rest + ". ("+totalRebate+" - " + kcRebate + " - " + investRebate + ")");
				}
				
			}			
        }catch (Exception e){
        	e.printStackTrace();
        }finally{
        	DBUtil.close(conn);
        	DBUtil.disConnection(session);
        }
	}
	
	public void process(){
		//app-trade-etl于 2016-08-09 10:53:03 发布，每天13点开始执行返利回收任务，所以从13点开始循环遍历
		
		Calendar c = Calendar.getInstance();
		c.set(2016, 7, 9, 13, 00, 00); // 2016-08-09 13:00:00
		long today = new Date().getTime();
		
		Session session = null;
        Connection conn = null;
        try{
        	session = DBUtil.openssh();
        	conn = DBUtil.getConnection();
        	do{
    			//查询3个月（90天）前创建的用户Id
    			UserService us = new UserService();
    			List<Map<String, String>> userList = us.getUserListBefore(conn, c.getTime());
    			
    			//当天
    			String strToday = DateUtils.today(c.getTime());
    			//当天前90
    			String strDate = DateUtils.addDate(c.getTime(), -90);
    			
    			for(Map<String, String> userMap : userList){
    				String userId = userMap.get("id");
    				
    				//总返利
    				String queryRebateSum = "select IFNULL(sum(handleSum),0) as rebateSum from t_fundrecord where userId = " + userId + " and operateType in (800,801,802,803,804,806,820)"; 
    				BigDecimal rebateSum = BeanMapUtils.queryReabte(conn, queryRebateSum);
    				
    				//三个月前总返利： 查询用户3个月之前获取的返利    返利发放（800,801,802,803,804,806,820）
    				String queryTotale = "select IFNULL(sum(handleSum),0) as totalRebate from t_fundrecord where userId = " + userId + " and operateType in (800,801,802,803,804,806,820) and "
    						+ "recordTime <= '"+strDate + "'";
    				BigDecimal totalRebate = BeanMapUtils.queryReabte(conn, queryTotale);
    				
    				//已扣除返利：查询用户所有扣除的返利        返利扣除（811, 812, 814）
    				String queryKC = "select IFNULL(sum(handleSum),0) as kcRebate from t_fundrecord where userId = " + userId + " and operateType in (812, 814) and recordTime <= '"+strToday+"'";
    				BigDecimal kcRebate = BeanMapUtils.queryReabte(conn, queryKC);
    				
    				//已投资返利：查询用户此前所有使用返利的投资记录
    				String queryInvest = "select sum(t.investRebate) from (select IFNULL(sum(fanliAmount),0) as investRebate from t_invest where investTime <= '"+strToday+"' and investor = " + userId + " union all select IFNULL(sum(handleSum),0) as investRebate from t_fundrecord where operateType = 811  and userId = "+userId+" and recordTime <= '"+strDate+"') t ";
    				BigDecimal investRebate = BeanMapUtils.queryReabte(conn, queryInvest);
    				
    				//rebateSum - (totalRebate - kcRebate - investRebate)
    				BigDecimal rest = rebateSum.subtract((totalRebate.subtract(kcRebate).subtract(investRebate)));
    				System.out.println("【" + strToday+"】userId: "+userId+", 总返利："+ rebateSum + ", 剩余返利：" + rest + ". ("+totalRebate+" - " + kcRebate + " - " + investRebate + ")");
    			}			
    			c.roll(Calendar.DAY_OF_YEAR, true);
    			
    			System.out.println("\n\n\n\n\n\n\n\n");
    		}while(c.getTimeInMillis() < today);
        }catch (Exception e){
        	e.printStackTrace();
        }finally{
        	DBUtil.close(conn);
        	DBUtil.disConnection(session);
        }
	}
	
	public static void main(String[] args) {
		RebateBiz rebateBiz = new RebateBiz();
		rebateBiz.process5();
	}
}
