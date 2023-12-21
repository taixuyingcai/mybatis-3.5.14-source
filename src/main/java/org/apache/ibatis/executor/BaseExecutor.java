/*
 *    Copyright 2009-2023 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.ibatis.executor;

import static org.apache.ibatis.executor.ExecutionPlaceholder.EXECUTION_PLACEHOLDER;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.cache.impl.PerpetualCache;
import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.executor.statement.StatementUtil;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.logging.LogFactory;
import org.apache.ibatis.logging.jdbc.ConnectionLogger;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.mapping.ParameterMode;
import org.apache.ibatis.mapping.StatementType;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.factory.ObjectFactory;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.LocalCacheScope;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.transaction.Transaction;
import org.apache.ibatis.type.TypeHandlerRegistry;

/**
 * @author Clinton Begin
 */
public abstract class BaseExecutor implements Executor {
	
	private static final Log log = LogFactory.getLog(BaseExecutor.class);
	
	protected Transaction transaction;
	protected Executor wrapper;
	
	// 延迟加载队列
	protected ConcurrentLinkedQueue<DeferredLoad> deferredLoads;
	// 一级缓存, 用于缓存Executor查询结果
	protected PerpetualCache localCache;
	// 一级缓存，用于缓存输出类型的参数
	protected PerpetualCache localOutputParameterCache;
	protected Configuration configuration;
	
	// 记录嵌套查询的层数
	protected int queryStack;
	private boolean closed;
	
	protected BaseExecutor(Configuration configuration, Transaction transaction) {
		this.transaction = transaction;
		this.deferredLoads = new ConcurrentLinkedQueue<>();
		this.localCache = new PerpetualCache("LocalCache");
		this.localOutputParameterCache = new PerpetualCache("LocalOutputParameterCache");
		this.closed = false;
		this.configuration = configuration;
		this.wrapper = this;
	}
	
	@Override
	public Transaction getTransaction() {
		if (closed) {
			throw new ExecutorException("Executor was closed.");
		}
		return transaction;
	}
	
	@Override
	public void close(boolean forceRollback) {
		try {
			try {
				rollback(forceRollback);
			} finally {
				if (transaction != null) {
					transaction.close();
				}
			}
		} catch (SQLException e) {
			// Ignore. There's nothing that can be done at this point.
			log.warn("Unexpected exception on closing transaction.  Cause: " + e);
		} finally {
			transaction = null;
			deferredLoads = null;
			localCache = null;
			localOutputParameterCache = null;
			closed = true;
		}
	}
	
	@Override
	public boolean isClosed() {
		return closed;
	}
	
	@Override
	public int update(MappedStatement ms, Object parameter) throws SQLException {
		ErrorContext.instance().resource(ms.getResource()).activity("executing an update").object(ms.getId());
		if (closed) {
			throw new ExecutorException("Executor was closed.");
		}
		clearLocalCache();
		return doUpdate(ms, parameter);
	}
	
	// 处理批量sql
	@Override
	public List<BatchResult> flushStatements() throws SQLException {
		return flushStatements(false);
	}
	
	public List<BatchResult> flushStatements(boolean isRollBack) throws SQLException {
		if (closed) {
			throw new ExecutorException("Executor was closed.");
		}
		return doFlushStatements(isRollBack);
	}
	
	@Override
	public <E> List<E> query(MappedStatement ms, Object parameter, RowBounds rowBounds, ResultHandler resultHandler)
		throws SQLException {
		BoundSql boundSql = ms.getBoundSql(parameter);
		// 创建缓存key
		CacheKey key = createCacheKey(ms, parameter, rowBounds, boundSql);
		return query(ms, parameter, rowBounds, resultHandler, key, boundSql);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public <E> List<E> query(MappedStatement ms, Object parameter, RowBounds rowBounds, ResultHandler resultHandler,
		CacheKey key, BoundSql boundSql) throws SQLException {
		// 设置错误上下文
		ErrorContext.instance().resource(ms.getResource()).activity("executing a query").object(ms.getId());
		if (closed) {
			throw new ExecutorException("Executor was closed.");
		}
		if (queryStack == 0 && ms.isFlushCacheRequired()) {
			// 如果没有嵌套查询，且<select>节点有flushCache属性，则清空一级缓存
			// flushCache属性是影响一级缓存的一个方面
			clearLocalCache();
		}
		List<E> list;
		try {
			// 增加查询层数
			queryStack++;
			// 查询一级缓存
			list = resultHandler == null ? (List<E>) localCache.getObject(key) : null;
			if (list != null) {
				// 对存储过程的处理，如果命中缓存，则取出缓存中的输出类型参数，并设置到用户传入的实参中
				handleLocallyCachedOutputParameters(ms, key, parameter, boundSql);
			} else {
				// 会调用doQuery()方法查询数据库
				list = queryFromDatabase(ms, parameter, rowBounds, resultHandler, key, boundSql);
			}
		} finally {
			// 完成查询后查询层数减一
			queryStack--;
		}
		if (queryStack == 0) {
			// 所有查询都执行完毕，相关缓存项也已经完全加载，所以这里可以这里可以触发DeferredLoad, 加载一级缓存中记录的嵌套查询的结果对象
			// 延时加载
			for (DeferredLoad deferredLoad : deferredLoads) {
				deferredLoad.load();
			}
			// issue #601
			// 清空延迟加载队列
			deferredLoads.clear();
			if (configuration.getLocalCacheScope() == LocalCacheScope.STATEMENT) {
				// issue #482
				// 根据localCacheScope的配置，清空一级缓存, localCacheScope是影响一级缓存的第二个方面
				clearLocalCache();
			}
		}
		return list;
	}
	
	@Override
	public <E> Cursor<E> queryCursor(MappedStatement ms, Object parameter, RowBounds rowBounds) throws SQLException {
		BoundSql boundSql = ms.getBoundSql(parameter);
		return doQueryCursor(ms, parameter, rowBounds, boundSql);
	}
	
	@Override
	public void deferLoad(MappedStatement ms, MetaObject resultObject, String property, CacheKey key,
		Class<?> targetType) {
		if (closed) {
			throw new ExecutorException("Executor was closed.");
		}
		// 创建DeferredLoad对象
		DeferredLoad deferredLoad = new DeferredLoad(resultObject, property, key, localCache, configuration, targetType);
		if (deferredLoad.canLoad()) {
			// 一级缓存中命中，从缓存加载并设置到外层对象中
			deferredLoad.load();
		} else {
			// 将上面创建的DeferredLoad对象添加到延迟加载队列中, 待整个外层查询结束后再进行加载
			deferredLoads.add(new DeferredLoad(resultObject, property, key, localCache, configuration, targetType));
		}
	}
	
	@Override
	public CacheKey createCacheKey(MappedStatement ms, Object parameterObject, RowBounds rowBounds, BoundSql boundSql) {
		if (closed) {
			throw new ExecutorException("Executor was closed.");
		}
		CacheKey cacheKey = new CacheKey();
		// 1. mappedStatement的id
		cacheKey.update(ms.getId());
		// 2. offset
		cacheKey.update(rowBounds.getOffset());
		// 3. limit
		cacheKey.update(rowBounds.getLimit());
		// 4. sql
		cacheKey.update(boundSql.getSql());
		List<ParameterMapping> parameterMappings = boundSql.getParameterMappings();
		TypeHandlerRegistry typeHandlerRegistry = ms.getConfiguration().getTypeHandlerRegistry();
		// mimic DefaultParameterHandler logic
		MetaObject metaObject = null;
		for (ParameterMapping parameterMapping : parameterMappings) {
			if (parameterMapping.getMode() != ParameterMode.OUT) {
				Object value;
				String propertyName = parameterMapping.getProperty();
				if (boundSql.hasAdditionalParameter(propertyName)) {
					value = boundSql.getAdditionalParameter(propertyName);
				} else if (parameterObject == null) {
					value = null;
				} else if (typeHandlerRegistry.hasTypeHandler(parameterObject.getClass())) {
					value = parameterObject;
				} else {
					if (metaObject == null) {
						metaObject = configuration.newMetaObject(parameterObject);
					}
					value = metaObject.getValue(propertyName);
				}
				// 5. 参数值
				cacheKey.update(value);
			}
		}
		if (configuration.getEnvironment() != null) {
			// issue #176
			// 6. 环境id
			cacheKey.update(configuration.getEnvironment().getId());
		}
		// 整个cacheKey由以上6部分组成
		return cacheKey;
	}
	
	@Override
	public boolean isCached(MappedStatement ms, CacheKey key) {
		// 检测缓存中是否缓存了cacheKey对应的对象
		return localCache.getObject(key) != null;
	}
	
	@Override
	public void commit(boolean required) throws SQLException {
		if (closed) {
			throw new ExecutorException("Cannot commit, transaction is already closed");
		}
		clearLocalCache();
		flushStatements();
		if (required) {
			transaction.commit();
		}
	}
	
	@Override
	public void rollback(boolean required) throws SQLException {
		if (!closed) {
			try {
				clearLocalCache();
				flushStatements(true);
			} finally {
				if (required) {
					transaction.rollback();
				}
			}
		}
	}
	
	@Override
	public void clearLocalCache() {
		if (!closed) {
			localCache.clear();
			localOutputParameterCache.clear();
		}
	}
	
	protected abstract int doUpdate(MappedStatement ms, Object parameter) throws SQLException;
	
	protected abstract List<BatchResult> doFlushStatements(boolean isRollback) throws SQLException;
	
	protected abstract <E> List<E> doQuery(MappedStatement ms, Object parameter, RowBounds rowBounds,
		ResultHandler resultHandler, BoundSql boundSql) throws SQLException;
	
	protected abstract <E> Cursor<E> doQueryCursor(MappedStatement ms, Object parameter, RowBounds rowBounds,
		BoundSql boundSql) throws SQLException;
	
	protected void closeStatement(Statement statement) {
		if (statement != null) {
			try {
				statement.close();
			} catch (SQLException e) {
				// ignore
			}
		}
	}
	
	/**
	 * Apply a transaction timeout.
	 *
	 * @param statement a current statement
	 * @throws SQLException if a database access error occurs, this method is called on a closed <code>Statement</code>
	 * @see StatementUtil#applyTransactionTimeout(Statement, Integer, Integer)
	 * @since 3.4.0
	 */
	protected void applyTransactionTimeout(Statement statement) throws SQLException {
		StatementUtil.applyTransactionTimeout(statement, statement.getQueryTimeout(), transaction.getTimeout());
	}
	
	private void handleLocallyCachedOutputParameters(MappedStatement ms, CacheKey key, Object parameter,
		BoundSql boundSql) {
		if (ms.getStatementType() == StatementType.CALLABLE) {
			final Object cachedParameter = localOutputParameterCache.getObject(key);
			if (cachedParameter != null && parameter != null) {
				final MetaObject metaCachedParameter = configuration.newMetaObject(cachedParameter);
				final MetaObject metaParameter = configuration.newMetaObject(parameter);
				for (ParameterMapping parameterMapping : boundSql.getParameterMappings()) {
					if (parameterMapping.getMode() != ParameterMode.IN) {
						final String parameterName = parameterMapping.getProperty();
						final Object cachedValue = metaCachedParameter.getValue(parameterName);
						metaParameter.setValue(parameterName, cachedValue);
					}
				}
			}
		}
	}
	
	private <E> List<E> queryFromDatabase(MappedStatement ms, Object parameter, RowBounds rowBounds,
		ResultHandler resultHandler, CacheKey key, BoundSql boundSql) throws SQLException {
		List<E> list;
		// 查询数据库之前先添加一个占位符，防止缓存穿透
		localCache.putObject(key, EXECUTION_PLACEHOLDER);
		try {
			// 执行数据库的查询
			list = doQuery(ms, parameter, rowBounds, resultHandler, boundSql);
		} finally {
			// 删除占位符
			localCache.removeObject(key);
		}
		// 将查询结果放入缓存
		localCache.putObject(key, list);
		if (ms.getStatementType() == StatementType.CALLABLE) {
			// 如果是存储过程类型的查询
			// 将输出类型的参数放入缓存
			localOutputParameterCache.putObject(key, parameter);
		}
		return list;
	}
	
	protected Connection getConnection(Log statementLog) throws SQLException {
		Connection connection = transaction.getConnection();
		if (statementLog.isDebugEnabled()) {
			return ConnectionLogger.newInstance(connection, statementLog, queryStack);
		}
		return connection;
	}
	
	@Override
	public void setExecutorWrapper(Executor wrapper) {
		this.wrapper = wrapper;
	}
	
	private static class DeferredLoad {
		
		// 外层对象对应的MetaObject
		private final MetaObject resultObject;
		// 延迟加载的属性名称
		private final String property;
		private final Class<?> targetType;
		private final CacheKey key;
		// 一级缓存， 与BaseExecutor.localCache是同一个对象
		private final PerpetualCache localCache;
		private final ObjectFactory objectFactory;
		// 负责结果转换
		private final ResultExtractor resultExtractor;
		
		// issue #781
		public DeferredLoad(MetaObject resultObject, String property, CacheKey key, PerpetualCache localCache,
			Configuration configuration, Class<?> targetType) {
			this.resultObject = resultObject;
			this.property = property;
			this.key = key;
			this.localCache = localCache;
			this.objectFactory = configuration.getObjectFactory();
			this.resultExtractor = new ResultExtractor(configuration, objectFactory);
			this.targetType = targetType;
		}
		
		// 检测缓存项是否已经完全加载
		// 完全加载：因为在BaseExecutor的queryFromDatabase()中，开始调用doQuery()方法查询数据库前，会先在localCache中添加占位符，等查询完成后，才将查询结果放入localCache中
		public boolean canLoad() {
			return localCache.getObject(key) != null && localCache.getObject(key) != EXECUTION_PLACEHOLDER;
		}
		
		public void load() {
			@SuppressWarnings("unchecked")
			// we suppose we get back a List
			List<Object> list = (List<Object>) localCache.getObject(key);
			Object value = resultExtractor.extractObjectFromList(list, targetType);
			resultObject.setValue(property, value);
		}
		
	}
	
}
