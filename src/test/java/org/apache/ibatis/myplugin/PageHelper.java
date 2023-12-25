package org.apache.ibatis.myplugin;

import java.util.Properties;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.plugin.Signature;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;

/**
 * @Description 分页插件
 * @Date 2023年12月21日 下午6:17
 * @Author shirq
 */
@Intercepts({
	@Signature(type = Executor.class, method = "query", args = {MappedStatement.class, Object.class, RowBounds.class,
		ResultHandler.class})
})
public class PageHelper implements Interceptor {
	
	private final static int STATEMENT_INDEX = 0;
	private final static int PARAMETER_INDEX = 1;
	private final static int ROW_BOUNDS_INDEX = 2;
	
	private Dialect dialect;
	
	@Override
	public Object intercept(Invocation invocation) throws Throwable {
		Object[] args = invocation.getArgs();
		final MappedStatement mappedStatement = (MappedStatement) args[STATEMENT_INDEX];
		final Object parameter = args[PARAMETER_INDEX];
		final RowBounds rowBounds = (RowBounds) args[ROW_BOUNDS_INDEX];
		int offset = rowBounds.getOffset();
		int limit = rowBounds.getLimit();
		BoundSql boundSql = mappedStatement.getBoundSql(parameter);
		final StringBuffer bufferSql = new StringBuffer(boundSql.getSql().trim());
		String sql = getFormatSql(bufferSql.toString().trim());
		if (dialect.supportPage()) {
			sql = dialect.getPagingSql(bufferSql.toString(), offset, limit);
			args[ROW_BOUNDS_INDEX] = new RowBounds(RowBounds.NO_ROW_OFFSET, RowBounds.NO_ROW_LIMIT);
		}
		args[STATEMENT_INDEX] = createMappedStatement(mappedStatement, boundSql, sql);
		
		return invocation.proceed();
	}
	
	private String getFormatSql(String trim) {
		return null;
	}
	
	private MappedStatement createMappedStatement(MappedStatement mappedStatement, BoundSql boundSql, String sql) {
		return null;
	}
	
	@Override
	public void setProperties(Properties properties) {
	}
}
