package org.apache.ibatis.reflection.typeparam;

import java.util.Map;

/**
 * @Description A
 * @Date 2023年11月29日 上午10:30
 * @Author shirq
 */
public class ClassA<K, V> {

  protected Map<K, V> map;

  public static class SubClassA<T> extends ClassA<T, T> {
  }
}
