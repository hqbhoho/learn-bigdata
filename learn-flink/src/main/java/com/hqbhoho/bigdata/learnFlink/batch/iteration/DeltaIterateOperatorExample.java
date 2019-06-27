package com.hqbhoho.bigdata.learnFlink.batch.iteration;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.Optional;

/**
 * describe:
 * 增量迭代   以官网Propagate Minimum in Graph为例进行说明
 * refence: https://ci.apache.org/projects/flink/flink-docs-release-1.8/dev/batch/iterations.html
 * 顶点之间的关系
 * 1<===>2   2<===>3   2<===>4   3<===>4   5<===>6   5<===>7   6<===>7
 * 迭代结束条件
 * The default termination condition for delta iterations is specified by the empty workset convergence criterion and a maximum number of iterations.
 * The iteration will terminate when a produced next workset is empty or when the maximum number of iterations is reached
 * 1. 达到最大迭代次数
 * 2. new workset 为空
 * 3. 自定义收敛条件
 * <p>
 * results:
 * (3,1)
 * (7,5)
 * (6,5)
 * (1,1)
 * (5,5)
 * (2,1)
 * (4,1)
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/06/27
 */
public class DeltaIterateOperatorExample {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // init SolutionSet and workset
        DataSet<Tuple2<Long, Long>> initialSolutionSet = InitDeltalterateData.initialSolutionSet(env);
        // 获取节点之间的连接关系
        DataSet<Tuple2<Long, Long>> initialEdges = InitDeltalterateData.initialEdges(env);
        //  create a DeltaIteration
        //在initialSolutionSet上调用iterateDelta（initialWorkset，maxIterations，key）
        DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> it =
                initialSolutionSet.iterateDelta(initialSolutionSet, 10, 0);
        /**
         * 以顶点2为例，说明此处的迭代逻辑
         * 初始的workest SolutionSet 均为   (2,2)
         * edges: (2,1) (2,3) (2,4)
         * 第一次join(workest,edges) --> (2,2,1) (2,2,3) (2,2,4)
         * 第二次join(第一次join结果,SolutionSet)  --->  (2,2,1)(1,1)    (2,2,3)(3,3)     (2,2,4)(4,4)
         * if (second.f1 < first.f1) {
         *     out.collect(Tuple2.of(first.f0, second.f1));
         * }
         * 此处就会输出 (2,1)
         * 此处算法逻辑还不是很完善      仅供参考
         */
        DataSet<Tuple2<Long, Long>> changes = it.getWorkset()
                .join(initialEdges)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple3<Long, Long, Long>>() {
                    @Override
                    public Tuple3<Long, Long, Long> join(Tuple2<Long, Long> first, Tuple2<Long, Long> second) throws Exception {
                        return Tuple3.of(first.f0, first.f1, second.f1);
                    }
                })
                .join(it.getSolutionSet())
                .where(2)
                .equalTo(0)
                .with(new FlatJoinFunction<Tuple3<Long, Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>>() {
                    @Override
                    public void join(Tuple3<Long, Long, Long> first, Tuple2<Long, Long> second, Collector<Tuple2<Long, Long>> out) throws Exception {
                        if (second.f1 < first.f1) {
                            Optional.ofNullable("process event: " + first + "====" + second).ifPresent(System.out::println);
                            out.collect(Tuple2.of(first.f0, second.f1));
                        }
                    }
                });// 此处可在做一次聚合   减少数据量
        // detlaSolutionSet  and   new workset
        DataSet<Tuple2<Long, Long>> result = it.closeWith(changes, changes);
        result.print();
    }
}
