//! The DAG order a steps class registers with: its stage and wire task
//! indices, which journals carry, so a change to the walk is a format change.

/// What a DAG pins: its order, parents and children, and each entry
/// channel's roots.
type Pinned = (
    Vec<u32>,
    Vec<Vec<usize>>,
    Vec<Vec<usize>>,
    Vec<(u32, Vec<usize>)>,
);

/// The DAG a host steps class registers with.
fn registered(id: &str, channels: &str, tasklets: &[&str]) -> Pinned {
    let tasklets: Vec<String> = tasklets
        .iter()
        .map(|name| format!(r#"{{"name":"{name}"}}"#))
        .collect();
    let json = format!(
        r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0","durable":false,"run":"steps",
             "channels":[{channels}],"tasklets":[{}]}}"#,
        tasklets.join(",")
    );
    crate::host::register_host_class_json(json.as_bytes()).expect("class registers");
    let class = crate::relay::registry::get_class(id).expect("class registered");
    let dag = &class.steps.as_ref().expect("a steps class").dag;
    let roots = dag
        .entries
        .iter()
        .map(|(channel, chain)| (*channel, chain.roots.clone()))
        .collect();
    (
        dag.order.clone(),
        dag.parents.clone(),
        dag.children.clone(),
        roots,
    )
}

#[test]
fn a_tree_is_numbered_level_by_level_in_edge_order() {
    let pinned = registered(
        "spec_order_tree",
        r#"{"name":"in","kind":"entry","chains":[[{"link":"into","tasklet":0}]]},
           {"name":"mid","kind":"inter_task","producer":0,
            "chains":[[{"link":"into","tasklet":3}],[{"link":"into","tasklet":2}]]},
           {"name":"x","kind":"inter_task","producer":3,"chains":[[{"link":"into","tasklet":1}]]},
           {"name":"y","kind":"inter_task","producer":2,"chains":[[{"link":"into","tasklet":4}]]}"#,
        &["root", "c", "a", "b", "d"],
    );
    let parents = vec![vec![], vec![0], vec![0], vec![1], vec![2]];
    let children = vec![vec![1, 2], vec![3], vec![4], vec![], vec![]];
    let roots = vec![(0, vec![0])];
    assert_eq!(pinned, (vec![0, 3, 2, 1, 4], parents, children, roots));
}

#[test]
fn a_diamond_numbers_its_join_after_both_producers() {
    let pinned = registered(
        "spec_order_diamond",
        r#"{"name":"in","kind":"entry","chains":[[{"link":"into","tasklet":0}]]},
           {"name":"fan","kind":"inter_task","producer":0,
            "chains":[[{"link":"into","tasklet":3}],[{"link":"into","tasklet":2}]]},
           {"name":"l","kind":"inter_task","producer":2,"chains":[[{"link":"into","tasklet":1}]]},
           {"name":"r","kind":"inter_task","producer":3,"chains":[[{"link":"into","tasklet":1}]]}"#,
        &["top", "join", "left", "right"],
    );
    let parents = vec![vec![], vec![0], vec![0], vec![1, 2]];
    let children = vec![vec![1, 2], vec![3], vec![3], vec![]];
    let roots = vec![(0, vec![0])];
    assert_eq!(pinned, (vec![0, 3, 2, 1], parents, children, roots));
}

/// The walk starts at the nodes no edge enters, in declaration order, so an
/// entry declared first does not number its tasklet first, and a tasklet an
/// entry and a producer both feed still follows that producer.
#[test]
fn two_entries_number_their_tasklets_in_declaration_order() {
    let pinned = registered(
        "spec_order_two_entries",
        r#"{"name":"b","kind":"entry","chains":[[{"link":"into","tasklet":1}]]},
           {"name":"a","kind":"entry","chains":[[{"link":"into","tasklet":0}]]},
           {"name":"o","kind":"inter_task","producer":0,"chains":[[{"link":"into","tasklet":1}]]}"#,
        &["x", "y"],
    );
    let roots = vec![(0, vec![1]), (1, vec![0])];
    assert_eq!(
        pinned,
        (
            vec![0, 1],
            vec![vec![], vec![0]],
            vec![vec![1], vec![]],
            roots
        )
    );
}
