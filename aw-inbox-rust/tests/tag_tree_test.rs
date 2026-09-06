//! 层级 tag（⑦-A）核心语义回归测试：
//!  - GET /inbox/notes?tag=<路径> 的段边界前缀匹配（`项目` 命中 `项目` 与 `项目/...`，不命中 `项目2`）
//!  - GET /inbox/tags/tree 的树结构与「含子孙」前缀计数
//!
//! 直接打 db 层（内存库），不依赖 HTTP。旧的 integration_test / integration_http_test
//! 引用了已移除的 axum/reqwest 依赖，note_crud_test 依赖 Unix shell —— 均与本测试无关。

use aw_inbox_rust::db;
use aw_inbox_rust::models::CreateNotePayload;
use rusqlite::Connection;

fn payload(content: &str, tags: &[&str]) -> CreateNotePayload {
    CreateNotePayload {
        content: content.to_string(),
        tags: Some(tags.iter().map(|s| s.to_string()).collect()),
        created_at: None,
    }
}

fn seed() -> Connection {
    let mut conn = Connection::open_in_memory().unwrap();
    db::migrate(&conn).unwrap();
    db::create_note_db(&mut conn, payload("a", &["项目/工作/ActivityWatch"]), None).unwrap();
    db::create_note_db(&mut conn, payload("b", &["项目/工作"]), None).unwrap();
    db::create_note_db(&mut conn, payload("c", &["项目/生活"]), None).unwrap();
    db::create_note_db(&mut conn, payload("d", &["项目2"]), None).unwrap();
    db::create_note_db(&mut conn, payload("e", &["其他"]), None).unwrap();
    conn
}

fn titles_with_tag(conn: &Connection, tag: Option<&str>) -> Vec<String> {
    let notes = db::get_notes_db(conn, None, None, tag.map(String::from), None, None, None, None)
        .unwrap();
    let mut v: Vec<String> = notes.into_iter().map(|n| n.content).collect();
    v.sort();
    v
}

#[test]
fn tag_filter_is_segment_boundary_prefix_match() {
    let conn = seed();

    // 无筛选 = 全量
    assert_eq!(titles_with_tag(&conn, None).len(), 5);
    // `项目` 命中自己 + 全部子孙，不含 `项目2`
    assert_eq!(
        titles_with_tag(&conn, Some("项目")),
        vec!["a", "b", "c"]
    );
    // 二级路径
    assert_eq!(titles_with_tag(&conn, Some("项目/工作")), vec!["a", "b"]);
    // 三级路径（精确到叶子）
    assert_eq!(
        titles_with_tag(&conn, Some("项目/工作/ActivityWatch")),
        vec!["a"]
    );
    // 兄弟段不互串：`项目/生` 不命中 `项目/生活`
    assert!(titles_with_tag(&conn, Some("项目/生")).is_empty());
}

fn find_opt<'a>(
    tree: &'a [aw_inbox_rust::models::TagNode],
    path: &str,
) -> Option<&'a aw_inbox_rust::models::TagNode> {
    tree.iter()
        .find_map(|node| find_opt(&node.children, path).or(if node.path == path { Some(node) } else { None }))
}

fn find<'a>(
    tree: &'a [aw_inbox_rust::models::TagNode],
    path: &str,
) -> &'a aw_inbox_rust::models::TagNode {
    find_opt(tree, path).unwrap_or_else(|| panic!("node {} not in tree", path))
}

#[test]
fn tag_tree_is_built_with_inclusive_counts() {
    let conn = seed();
    let tree = db::get_tag_tree_db(&conn).unwrap();

    // 根：即使没人直接打过 `项目`（它只是中间路径），也必须作为根出现
    let root_paths: Vec<&str> = tree.iter().map(|n| n.path.as_str()).collect();
    assert_eq!(root_paths, vec!["其他", "项目", "项目2"]);

    // 项目 count = 全部子孙的笔记数 = 3
    let project = find(&tree, "项目");
    assert_eq!(project.count, 3);
    let children: Vec<(&str, i64)> = project
        .children
        .iter()
        .map(|c| (c.path.as_str(), c.count))
        .collect();
    assert_eq!(children, vec![("项目/工作", 2), ("项目/生活", 1)]);
    let work = find(&tree, "项目/工作");
    assert_eq!(
        work.children.iter().map(|c| (c.path.as_str(), c.count)).collect::<Vec<_>>(),
        vec![("项目/工作/ActivityWatch", 1)]
    );
    assert_eq!(find(&tree, "其他").count, 1);
    assert_eq!(find(&tree, "项目2").count, 1);
}

#[test]
fn deleted_notes_are_excluded_from_filter_and_tree() {
    let mut conn = seed();
    // 删除 tag=其他 的那条（e），筛选与树都不应再算它
    let others = db::get_notes_db(&conn, None, None, Some("其他".into()), None, None, None, None)
        .unwrap();
    let id = others[0].id;
    assert!(db::delete_note_db(&mut conn, id, None).unwrap());

    assert!(titles_with_tag(&conn, Some("其他")).is_empty());
    let tree = db::get_tag_tree_db(&conn).unwrap();
    assert!(!tree.iter().any(|n| n.path == "其他"));
    let project = tree.iter().find(|n| n.path == "项目").expect("项目 root");
    assert_eq!(project.count, 3); // 项目 分支不受影响
}
