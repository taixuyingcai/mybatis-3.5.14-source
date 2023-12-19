/*
 *    Copyright 2009-2022 the original author or authors.
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

/**
 * @author Clinton Begin
 * sql语句中的动态节点的接口
 */
public interface SqlNode {
  // apply方法是SqlNode中定义的唯一方法，该方法会根据用户传入的实参，参数解析SqlNode中的动态节点
  // 并调用DynamicContext中的appendSql方法，将解析后的sql片段加入到DynamicContext.sqlBuilder中
  // 当SQL下的所有sqlNode解析完成后，就可以在DynamicContext中获得一条完整的sql语句
  boolean apply(DynamicContext context);
}
