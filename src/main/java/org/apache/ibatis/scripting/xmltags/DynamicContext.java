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
package org.apache.ibatis.scripting.xmltags;

import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

import ognl.OgnlContext;
import ognl.OgnlRuntime;
import ognl.PropertyAccessor;

import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.session.Configuration;

/**
 * @author Clinton Begin
 */
// 记录解析动态sql后产生的sql语句片段
  // 可以认为是一个记录动态sql语句解析结果的窗口
public class DynamicContext {

  public static final String PARAMETER_OBJECT_KEY = "_parameter";
  public static final String DATABASE_ID_KEY = "_databaseId";

  static {
    OgnlRuntime.setPropertyAccessor(ContextMap.class, new ContextAccessor());
  }

  // 参数上下文
  private final ContextMap bindings;
  // 解析后的sql语句,解析过程中会将解析后的sql片段添加到这里，最终会将解析后的sql片段拼接成完整的sql语句
  private final StringJoiner sqlBuilder = new StringJoiner(" ");
  private int uniqueNumber;

  // parameterObject是运行时用户传入的参数
  public DynamicContext(Configuration configuration, Object parameterObject) {
    if (parameterObject != null && !(parameterObject instanceof Map)) {
      // 如果参数不是map类型，则创建对应的MetaObject, 并封装成ContextMap
      MetaObject metaObject = configuration.newMetaObject(parameterObject);
      boolean existsTypeHandler = configuration.getTypeHandlerRegistry().hasTypeHandler(parameterObject.getClass());
      // 初始化bindings集合
      bindings = new ContextMap(metaObject, existsTypeHandler);
    } else {
      bindings = new ContextMap(null, false);
    }
    // 将"_parameter"添加到bindings集合中, 在有的sqlNode中会使用
    bindings.put(PARAMETER_OBJECT_KEY, parameterObject);
    bindings.put(DATABASE_ID_KEY, configuration.getDatabaseId());
  }

  public Map<String, Object> getBindings() {
    return bindings;
  }

  public void bind(String name, Object value) {
    bindings.put(name, value);
  }

  public void appendSql(String sql) {
    sqlBuilder.add(sql);
  }

  public String getSql() {
    return sqlBuilder.toString().trim();
  }

  public int getUniqueNumber() {
    return uniqueNumber++;
  }

  static class ContextMap extends HashMap<String, Object> {
    private static final long serialVersionUID = 2977601501966151582L;
    private final MetaObject parameterMetaObject;
    private final boolean fallbackParameterObject;

    public ContextMap(MetaObject parameterMetaObject, boolean fallbackParameterObject) {
      this.parameterMetaObject = parameterMetaObject;
      this.fallbackParameterObject = fallbackParameterObject;
    }

    @Override
    public Object get(Object key) {
      String strKey = (String) key;
      if (super.containsKey(strKey)) {
        return super.get(strKey);
      }

      if (parameterMetaObject == null) {
        return null;
      }

      if (fallbackParameterObject && !parameterMetaObject.hasGetter(strKey)) {
        return parameterMetaObject.getOriginalObject();
      }
      // issue #61 do not modify the context when reading
      return parameterMetaObject.getValue(strKey);
    }
  }

  static class ContextAccessor implements PropertyAccessor {

    @Override
    public Object getProperty(Map context, Object target, Object name) {
      Map map = (Map) target;

      Object result = map.get(name);
      if (map.containsKey(name) || result != null) {
        return result;
      }

      Object parameterObject = map.get(PARAMETER_OBJECT_KEY);
      if (parameterObject instanceof Map) {
        return ((Map) parameterObject).get(name);
      }

      return null;
    }

    @Override
    public void setProperty(Map context, Object target, Object name, Object value) {
      Map<Object, Object> map = (Map<Object, Object>) target;
      map.put(name, value);
    }

    @Override
    public String getSourceAccessor(OgnlContext arg0, Object arg1, Object arg2) {
      return null;
    }

    @Override
    public String getSourceSetter(OgnlContext arg0, Object arg1, Object arg2) {
      return null;
    }
  }
}
