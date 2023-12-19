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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.ibatis.session.Configuration;

/**
 * @author Clinton Begin
 */
public class TrimSqlNode implements SqlNode {

  // trim节点的子节点
  private final SqlNode contents;
  // 前缀
  private final String prefix;
  // 后缀
  private final String suffix;
  // 前缀覆盖 如果trim节点中的sql语句是空的，则删除前缀
  private final List<String> prefixesToOverride;
  // 后缀覆盖 如果trim节点中的sql语句是空的，则删除后缀
  private final List<String> suffixesToOverride;
  private final Configuration configuration;

  public TrimSqlNode(Configuration configuration, SqlNode contents, String prefix, String prefixesToOverride,
      String suffix, String suffixesToOverride) {
    this(configuration, contents, prefix, parseOverrides(prefixesToOverride), suffix,
        parseOverrides(suffixesToOverride));
  }

  protected TrimSqlNode(Configuration configuration, SqlNode contents, String prefix, List<String> prefixesToOverride,
      String suffix, List<String> suffixesToOverride) {
    this.contents = contents;
    this.prefix = prefix;
    this.prefixesToOverride = prefixesToOverride;
    this.suffix = suffix;
    this.suffixesToOverride = suffixesToOverride;
    this.configuration = configuration;
  }

  @Override
  public boolean apply(DynamicContext context) {
    // 创建FilteredDynamicContext, 此类继承自DynamicContext, 且内部封装了DynamicContext
    FilteredDynamicContext filteredDynamicContext = new FilteredDynamicContext(context);
    // 处理子节点
    boolean result = contents.apply(filteredDynamicContext);
    // 处理前缀和后缀
    filteredDynamicContext.applyAll();
    return result;
  }

  // 解析prefixesToOverride和suffixesToOverride，并且初始化
  private static List<String> parseOverrides(String overrides) {
    if (overrides != null) {
      final StringTokenizer parser = new StringTokenizer(overrides, "|", false);
      final List<String> list = new ArrayList<>(parser.countTokens());
      while (parser.hasMoreTokens()) {
        list.add(parser.nextToken().toUpperCase(Locale.ENGLISH));
      }
      return list;
    }
    return Collections.emptyList();
  }

  private class FilteredDynamicContext extends DynamicContext {
    private final DynamicContext delegate;
    private boolean prefixApplied;
    private boolean suffixApplied;
    private StringBuilder sqlBuffer;

    public FilteredDynamicContext(DynamicContext delegate) {
      super(configuration, null);
      this.delegate = delegate;
      this.prefixApplied = false;
      this.suffixApplied = false;
      this.sqlBuffer = new StringBuilder();
    }

    public void applyAll() {
      // 获取子节点的解析结果
      sqlBuffer = new StringBuilder(sqlBuffer.toString().trim());
      String trimmedUppercaseSql = sqlBuffer.toString().toUpperCase(Locale.ENGLISH);
      if (trimmedUppercaseSql.length() > 0) {
        // 处理前缀和后缀
        applyPrefix(sqlBuffer, trimmedUppercaseSql);
        applySuffix(sqlBuffer, trimmedUppercaseSql);
      }
      delegate.appendSql(sqlBuffer.toString());
    }

    @Override
    public Map<String, Object> getBindings() {
      return delegate.getBindings();
    }

    @Override
    public void bind(String name, Object value) {
      delegate.bind(name, value);
    }

    @Override
    public int getUniqueNumber() {
      return delegate.getUniqueNumber();
    }

    @Override
    public void appendSql(String sql) {
      sqlBuffer.append(sql);
    }

    @Override
    public String getSql() {
      return delegate.getSql();
    }

    private void applyPrefix(StringBuilder sql, String trimmedUppercaseSql) {
      // 检查是否已经处理过前缀
      if (prefixApplied) {
        return;
      }
      // 标记已经处理过前缀
      prefixApplied = true;
      // 如果前缀覆盖集合不为空
      if (prefixesToOverride != null) {
        // 找到sql中以前缀集合中的前缀开头的第一个，然后删除
        prefixesToOverride.stream().filter(trimmedUppercaseSql::startsWith).findFirst()
            .ifPresent(toRemove -> sql.delete(0, toRemove.trim().length()));
      }
      if (prefix != null) {
        // 添加prefix前缀
        sql.insert(0, " ").insert(0, prefix);
      }
    }

    private void applySuffix(StringBuilder sql, String trimmedUppercaseSql) {
      // 检查是否已经处理过后缀
      if (suffixApplied) {
        return;
      }
      // 标记已经处理过后缀
      suffixApplied = true;
      if (suffixesToOverride != null) {
        // 找到sql中以后缀集合中的后缀结尾的第一个，然后删除
        suffixesToOverride.stream()
            .filter(toRemove -> trimmedUppercaseSql.endsWith(toRemove) || trimmedUppercaseSql.endsWith(toRemove.trim()))
            .findFirst().ifPresent(toRemove -> {
              int start = sql.length() - toRemove.trim().length();
              int end = sql.length();
              sql.delete(start, end);
            });
      }
      if (suffix != null) {
        // 添加suffix后缀
        sql.append(" ").append(suffix);
      }
    }

  }

}
