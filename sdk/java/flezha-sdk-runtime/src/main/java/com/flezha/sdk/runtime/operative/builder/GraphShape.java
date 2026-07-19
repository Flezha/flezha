package com.flezha.sdk.runtime.operative.builder;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * The shared graph-shape walk (`sdk-graph-authoring`): Kahn topological order
 * over the operation nodes plus reachability from the entry. One walk serves
 * both the unbounded topology validation and the bounded execution-order
 * construction — the shape rule is one rule, so its mechanism is one
 * mechanism. Mirrors the Rust SDK's {@code operative::builder::graph_order}.
 */
public final class GraphShape {

    /**
     * @param order node ids in Kahn topological order (FIFO — for a tree this
     *     is exactly BFS from the root, so bounded {@code op_idx} numbering
     *     stays stable)
     * @param pos node id → position in {@code order}; {@code -1} for a node on
     *     a cycle
     * @param unreachable first node unreachable from the roots, or {@code -1}
     *     (meaningful only when acyclic — a cycle already explains
     *     unreachability)
     * @param onCycle first node Kahn left over — on (or strictly downstream
     *     of) a cycle — or {@code -1}
     */
    public record Order(List<Integer> order, int[] pos, int unreachable, int onCycle) {}

    private GraphShape() {}

    /**
     * Walk {@code n} nodes over {@code edges} (producer → consumer pairs,
     * one per channel-consumer), rooted at {@code roots} (the entry channel's
     * consumers).
     */
    @Nonnull
    public static Order order(int n, @Nonnull List<int[]> edges, @Nonnull List<Integer> roots) {
        int[] indegree = new int[n];
        List<List<Integer>> adjacency = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            adjacency.add(new ArrayList<>());
        }
        for (int[] edge : edges) {
            adjacency.get(edge[0]).add(edge[1]);
            indegree[edge[1]]++;
        }

        boolean[] reachable = new boolean[n];
        Deque<Integer> stack = new ArrayDeque<>(roots);
        while (!stack.isEmpty()) {
            int node = stack.pop();
            if (reachable[node]) {
                continue;
            }
            reachable[node] = true;
            stack.addAll(adjacency.get(node));
        }
        int unreachable = -1;
        for (int i = 0; i < n; i++) {
            if (!reachable[i]) {
                unreachable = i;
                break;
            }
        }

        Deque<Integer> queue = new ArrayDeque<>();
        for (int i = 0; i < n; i++) {
            if (indegree[i] == 0) {
                queue.add(i);
            }
        }
        List<Integer> order = new ArrayList<>(n);
        int[] pos = new int[n];
        java.util.Arrays.fill(pos, -1);
        while (!queue.isEmpty()) {
            int node = queue.poll();
            pos[node] = order.size();
            order.add(node);
            for (int child : adjacency.get(node)) {
                if (--indegree[child] == 0) {
                    queue.add(child);
                }
            }
        }
        int onCycle = -1;
        for (int i = 0; i < n; i++) {
            if (pos[i] < 0) {
                onCycle = i;
                break;
            }
        }

        return new Order(List.copyOf(order), pos, unreachable, onCycle);
    }
}
