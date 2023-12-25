package org.apache.ibatis.myplugin;

/**
 * @Description 数据库产品类型
 * @Date 2023年12月21日 下午6:27
 * @Author shirq
 */
public interface Dialect {
	
	// 是否支持分页
	boolean supportPage();
	
	// 返回最终的分页SQL
	String getPagingSql(String sql, int offset, int limit);
	
}
