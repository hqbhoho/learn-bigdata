package com.hqbhoho.bigdata.guava.collections;

import com.google.common.collect.*;
import org.junit.Test;

import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/09/16
 */
public class TestNewCollectionType {

    @Test
    /**
     * 可以多次添加相等的元素的集合
     * 可以理解为没有元素顺序限制的ArrayList<E>，Map<E, Integer>，键为元素，值为计数
     *
     * Guava提供了多种Multiset的实现，如
     * Map	                对应的Multiset	        是否支持null元素
     * HashMap	            HashMultiset	            是
     * TreeMap	            TreeMultiset	     是（如果comparator支持的话）
     * LinkedHashMap	    LinkedHashMultiset	        是
     * ConcurrentHashMap  ConcurrentHashMultiset	    否
     * ImmutableMap	        ImmutableMultiset	        否
     */
    public void testMultiset() {
        HashMultiset<Integer> ints = HashMultiset.create();
        ints.add(1);
        ints.add(2);
        ints.add(1);
        ints.add(2);
        ints.add(3);

        // 当把Multiset看成普通的Collection时，它表现得就像无序的ArrayList
        // 1,1,2,2,3   与插入顺序无关  无序
        ints.forEach(System.out::println);
        // 当把Multiset看作Map<E, Integer>时，它也提供了符合性能期望的查询操作
        assertThat(ints.count(1), equalTo(2));
    }

    @Test
    /**
     * 键可以对应任意多个值
     *
     * 实现	                    键行为类似	    值行为类似
     * ArrayListMultimap	    HashMap	        ArrayList
     * HashMultimap	            HashMap	        HashSet
     * LinkedListMultimap*	    LinkedHashMap*	LinkedList*
     * LinkedHashMultimap**	    LinkedHashMap	LinkedHashMap
     * TreeMultimap	            TreeMap	        TreeSet
     * ImmutableListMultimap	ImmutableMap	ImmutableList
     * ImmutableSetMultimap	    ImmutableMap	ImmutableSet
     */
    public void testArrayListMultimap() {
        Multimap<String, Integer> multimap = ArrayListMultimap.create();
        multimap.put("a", 1);
        multimap.put("a", 1);
        multimap.put("b", 3);
        multimap.put("b", 3);
        multimap.put("c", 5);

        assertThat(multimap.size(), equalTo(5));
        /**
         * a:1
         * a:1
         * b:3
         * b:3
         * c:5
         */
        multimap.forEach((k, v) -> Optional.ofNullable(k + ":" + v).ifPresent(System.out::println));

        assertThat(multimap.asMap().size(), equalTo(3));
        /**
         * a:[1, 1]
         * b:[3, 3]
         * c:[5]
         */
        multimap.asMap().forEach((k, v) -> Optional.ofNullable(k + ":" + v).ifPresent(System.out::println));
    }

    @Test
    public void testHashMultimap() {
        Multimap<String, Integer> multimap = HashMultimap.create();
        multimap.put("a", 1);
        multimap.put("a", 1);
        multimap.put("b", 3);
        multimap.put("b", 3);
        multimap.put("c", 5);

        assertThat(multimap.size(), equalTo(3));
        /**
         * a:1
         * b:3
         * c:5
         */
        multimap.forEach((k, v) -> Optional.ofNullable(k + ":" + v).ifPresent(System.out::println));

        assertThat(multimap.asMap().size(), equalTo(3));
        /**
         * a:[1]
         * b:[3]
         * c:[5]
         */
        multimap.asMap().forEach((k, v) -> Optional.ofNullable(k + ":" + v).ifPresent(System.out::println));
    }

    @Test
    /**
     * 实现键值对的双向映射的map
     * 可以用 inverse()反转BiMap<K, V>的键值映射
     * 保证值是唯一的，因此 values()返回Set而不是普通的Collection
     *
     * 键–值实现	        值–键实现	        对应的BiMap实现
     * HashMap	        HashMap	        HashBiMap
     * ImmutableMap	    ImmutableMap	ImmutableBiMap
     * EnumMap	        EnumMap	        EnumBiMap
     * EnumMap	        HashMap	        EnumHashBiMap
     */
    public void testBiMap() {
        HashBiMap<String, Integer> biMap = HashBiMap.create();

        biMap.put("a", 1);
        biMap.put("b", 2);
        biMap.put("c", 3);

        /**
         * a:1
         * b:2
         * c:3
         */
        biMap.forEach((k, v) -> Optional.ofNullable(k + ":" + v).ifPresent(System.out::println));

        BiMap<Integer, String> inverse = biMap.inverse();

        /**
         * 1:a
         * 2:b
         * 3:c
         */
        inverse.forEach((k, v) -> Optional.ofNullable(k + ":" + v).ifPresent(System.out::println));
    }

    @Test
    /**
     * 带有两个支持所有类型的键：”行”和”列”
     * Table有如下几种实现：
     * HashBasedTable：本质上用HashMap<R, HashMap<C, V>>实现；
     * TreeBasedTable：本质上用TreeMap<R, TreeMap<C,V>>实现；
     * ImmutableTable：本质上用ImmutableMap<R, ImmutableMap<C, V>>实现；注：ImmutableTable对稀疏或密集的数据集都有优化。
     * ArrayTable：要求在构造时就指定行和列的大小，本质上由一个二维数组实现，以提升访问速度和密集Table的内存利用率
     */
    public void testTable() {
        HashBasedTable<Integer, Integer, String> hashBasedTable = HashBasedTable.create();
        hashBasedTable.put(1, 1, "a");
        hashBasedTable.put(2, 2, "b");
        // {1={1=a}, 2={2=b}}
        System.out.println(hashBasedTable);
        Map<Integer, String> columnMap = hashBasedTable.columnMap().get(1);
        columnMap.put(1,"c");
        // {1={1=c}, 2={2=b}}
        System.out.println(hashBasedTable);
    }

    @Test
    /**
     * ClassToInstanceMap是一种特殊的Map：它的键是类型，而值是符合键所指类型的对象
     * 对于ClassToInstanceMap，Guava提供了两种有用的实现：MutableClassToInstanceMap和 ImmutableClassToInstanceMap
     */
    public void testClassToInstanceMap(){
        MutableClassToInstanceMap<Integer> mutableClassToInstanceMap = MutableClassToInstanceMap.create();
        mutableClassToInstanceMap.put(Integer.class,1);
        mutableClassToInstanceMap.put(Integer.class,3);
        // class java.lang.Integer:3
        mutableClassToInstanceMap.forEach((k, v) -> Optional.ofNullable(k + ":" + v).ifPresent(System.out::println));
    }

    @Test
    /**
     *  RangeSet描述了一组不相连的、非空的区间
     * 当把一个区间添加到可变的RangeSet时，所有相连的区间会被合并，空区间会被忽略
     */
    public void testRangeSet(){
        TreeRangeSet<Comparable<?>> rangeSet = TreeRangeSet.create();
        rangeSet.add(Range.closed(1,5));
        // [[1..5]]
        System.out.println(rangeSet);
        rangeSet.add(Range.closed(5,6));
        // [[1..6]]
        System.out.println(rangeSet);
        rangeSet.add(Range.closedOpen(7,10));
        // [[1..6], [7..10)]
        System.out.println(rangeSet);
        rangeSet.add(Range.closedOpen(10,15));
        // [[1..6], [7..15)]
        System.out.println(rangeSet);

        rangeSet.remove(Range.closed(8,9));
        //[[1..6], [7..8), (9..15)]
        System.out.println(rangeSet);

        // 判断RangeSet中是否有任何区间包含给定元素
        assertThat( rangeSet.contains(3),equalTo(true));

        // 返回包含给定元素的区间；若没有这样的区间，则返回null
        Optional.ofNullable(rangeSet.rangeContaining(3)).ifPresent(System.out::println);

        // 判断RangeSet中是否有任何区间包括给定区间
        assertThat(rangeSet.encloses(Range.closed(2,3)),equalTo(true));

        // 返回包括RangeSet中所有区间的最小区间
        Optional.ofNullable(rangeSet.span()).ifPresent(System.out::println);
    }

    @Test
    /**
     * RangeMap描述了”不相交的、非空的区间”到特定值的映射
     * 和RangeSet不同，RangeMap不会合并相邻的映射，即便相邻的区间映射到相同的值
     */
    public void testRangeMap(){
        RangeMap<Integer, String> rangeMap = TreeRangeMap.create();

        rangeMap.put(Range.closed(1, 10), "foo"); //{[1,10] => "foo"}
        System.out.println(rangeMap.toString());

        rangeMap.put(Range.open(3, 6), "bar"); //{[1,3] => "foo", (3,6) => "bar", [6,10] => "foo"}
        System.out.println(rangeMap.toString());

        rangeMap.put(Range.open(10, 20), "foo"); //{[1,3] => "foo", (3,6) => "bar", [6,10] => "foo", (10,20) => "foo"}
        System.out.println(rangeMap.toString());

        rangeMap.remove(Range.closed(5, 11)); //{[1,3] => "foo", (3,5) => "bar", (11,20) => "foo"}
        System.out.println(rangeMap.toString());
    }
}
