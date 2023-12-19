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

import java.util.Map;
import java.util.Optional;

import org.apache.ibatis.parsing.GenericTokenParser;
import org.apache.ibatis.session.Configuration;

/**
 * @author Clinton Begin
 */
public class ForEachSqlNode implements SqlNode {
  public static final String ITEM_PREFIX = "__frch_";

  // 用于判断循环条件的终止, forEachSqlNode构造方法中会创建该对象
  private final ExpressionEvaluator evaluator;
  // 迭代集合的collection表达式
  private final String collectionExpression;
  private final Boolean nullable;
  // foreach的子节点
  private final SqlNode contents;
  private final String open;
  private final String close;
  // 分隔符
  private final String separator;

  // index是迭代的次数，item是迭代的元素。若迭代元素是Map, 则index是键，item是值。
  private final String item;
  private final String index;
  private final Configuration configuration;

  /**
   * @deprecated Since 3.5.9, use the
   *             {@link #ForEachSqlNode(Configuration, SqlNode, String, Boolean, String, String, String, String, String)}.
   */
  @Deprecated
  public ForEachSqlNode(Configuration configuration, SqlNode contents, String collectionExpression, String index,
      String item, String open, String close, String separator) {
    this(configuration, contents, collectionExpression, null, index, item, open, close, separator);
  }

  /**
   * @since 3.5.9
   */
  public ForEachSqlNode(Configuration configuration, SqlNode contents, String collectionExpression, Boolean nullable,
      String index, String item, String open, String close, String separator) {
    this.evaluator = new ExpressionEvaluator();
    this.collectionExpression = collectionExpression;
    this.nullable = nullable;
    this.contents = contents;
    this.open = open;
    this.close = close;
    this.separator = separator;
    this.index = index;
    this.item = item;
    this.configuration = configuration;
  }

  @Override
  public boolean apply(DynamicContext context) {
    // 获取sql中的参数信息
    Map<String, Object> bindings = context.getBindings();
    // 解析collection属性的表达式
    final Iterable<?> iterable = evaluator.evaluateIterable(collectionExpression, bindings,
        Optional.ofNullable(nullable).orElseGet(configuration::isNullableOnForEach));
    // 如果集合为空，则直接返回
    if (iterable == null || !iterable.iterator().hasNext()) {
      return true;
    }
    boolean first = true;
    // 添加open指定的前缀
    applyOpen(context);
    int i = 0;
    // 循环collection集合中的元素
    for (Object o : iterable) {
      DynamicContext oldContext = context;
      if (first || separator == null) {
        // 如果是第一个元素或没有指定分隔符，使用默认的空中作为前缀
        // 在PrefixedContext中,如果没有做前缀处理，会在sql前追加前缀
        context = new PrefixedContext(context, "");
      } else {
        // 例如: <foreach collection="list" item="item" separator=","> 分隔符是“，” 每次处理item元素的时候都会在前面追加“，”
        context = new PrefixedContext(context, separator);
      }
      // 从0开始，每次++，用于转换生成新的#{}占位符
      int uniqueNumber = context.getUniqueNumber();
      // Issue #709
      if (o instanceof Map.Entry) {
        @SuppressWarnings("unchecked")
        Map.Entry<Object, Object> mapEntry = (Map.Entry<Object, Object>) o;
        applyIndex(context, mapEntry.getKey(), uniqueNumber);
        applyItem(context, mapEntry.getValue(), uniqueNumber);
      } else {
        applyIndex(context, i, uniqueNumber);
        applyItem(context, o, uniqueNumber);
      }
      contents.apply(new FilteredDynamicContext(configuration, context, index, item, uniqueNumber));
      if (first) {
        first = !((PrefixedContext) context).isPrefixApplied();
      }
      context = oldContext;
      i++;
    }
    applyClose(context);
    context.getBindings().remove(item);
    context.getBindings().remove(index);
    return true;
  }

  private void applyIndex(DynamicContext context, Object o, int i) {
    if (index != null) {
      context.bind(index, o);
      context.bind(itemizeItem(index, i), o);
    }
  }

  private void applyItem(DynamicContext context, Object o, int i) {
    if (item != null) {
      context.bind(item, o);
      context.bind(itemizeItem(item, i), o);
    }
  }

  private void applyOpen(DynamicContext context) {
    if (open != null) {
      context.appendSql(open);
    }
  }

  private void applyClose(DynamicContext context) {
    if (close != null) {
      context.appendSql(close);
    }
  }

  private static String itemizeItem(String item, int i) {
    return ITEM_PREFIX + item + "_" + i;
  }

  // 负责处理“${}”占位符
  private static class FilteredDynamicContext extends DynamicContext {
    private final DynamicContext delegate;
    private final int index;
    private final String itemIndex;
    private final String item;

    public FilteredDynamicContext(Configuration configuration, DynamicContext delegate, String itemIndex, String item,
        int i) {
      super(configuration, null);
      this.delegate = delegate;
      this.index = i;
      this.itemIndex = itemIndex;
      this.item = item;
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
    public String getSql() {
      return delegate.getSql();
    }

    /**
     * 处理${}占位符，将${item}替换为${__frch_item_0}，${itemIndex}替换为${__frch_itemIndex_0}
     * _frch_是固定前缀，item是迭代的元素，0是迭代的次数。
     */
    @Override
    public void appendSql(String sql) {
      // 创建GenericTokenParser, 注意这里的TokenHandler是匿名的
      GenericTokenParser parser = new GenericTokenParser("#{", "}", content -> {
        String newContent = content.replaceFirst("^\\s*" + item + "(?![^.,:\\s])", itemizeItem(item, index));
        if (itemIndex != null && newContent.equals(content)) {
          newContent = content.replaceFirst("^\\s*" + itemIndex + "(?![^.,:\\s])", itemizeItem(itemIndex, index));
        }
        return "#{" + newContent + "}";
      });

      delegate.appendSql(parser.parse(sql));
    }

    @Override
    public int getUniqueNumber() {
      return delegate.getUniqueNumber();
    }

  }

  // 处理前缀
  private class PrefixedContext extends DynamicContext {
    private final DynamicContext delegate;
    private final String prefix;
    private boolean prefixApplied;

    public PrefixedContext(DynamicContext delegate, String prefix) {
      super(configuration, null);
      this.delegate = delegate;
      this.prefix = prefix;
      this.prefixApplied = false;
    }

    public boolean isPrefixApplied() {
      return prefixApplied;
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
    public void appendSql(String sql) {
      if (!prefixApplied && sql != null && sql.trim().length() > 0) {
        // 追回前缀
        delegate.appendSql(prefix);
        prefixApplied = true;
      }
      // 追回sql
      delegate.appendSql(sql);
    }

    @Override
    public String getSql() {
      return delegate.getSql();
    }

    @Override
    public int getUniqueNumber() {
      return delegate.getUniqueNumber();
    }
  }

}
