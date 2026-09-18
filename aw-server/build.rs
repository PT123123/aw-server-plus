// build.rs —— 在 Windows 上为 aw-server.exe 内嵌程序图标。
//
// 任务管理器与资源管理器只显示 exe 内嵌的图标资源，缺了它就退化成系统通用图标。
// 这里用 Windows SDK 自带的 rc.exe 把 windows/app.rc 编成 .res，再原样交给 link.exe
// （link.exe 接受 .res 作为输入），从而不引入任何 crates.io 依赖。
//
// rc.exe 不在 PATH、或 app.ico 未就位时静默跳过：图标不影响服务端功能。
// app.ico 由上层 aw-qtui 仓库的 `just icon` 生成、`just server` 拷入。

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=windows/app.rc");
    println!("cargo:rerun-if-changed=windows/app.ico");
    embed_windows_icon();
}

#[cfg(windows)]
fn embed_windows_icon() {
    use std::path::Path;

    let rc_path = Path::new("windows/app.rc");
    if !rc_path.exists() || !Path::new("windows/app.ico").exists() {
        println!(
            "cargo:warning=windows/app.ico 未就位，跳过图标内嵌（在 aw-qtui 仓库先跑 `just icon`）"
        );
        return;
    }

    let out_dir = std::env::var("OUT_DIR").expect("OUT_DIR");
    let res_path = Path::new(&out_dir).join("aw-server-icon.res");

    match std::process::Command::new("rc.exe")
        .arg("/nologo")
        .arg("/fo")
        .arg(&res_path)
        .arg(rc_path)
        .status()
    {
        Ok(status) if status.success() => {
            println!("cargo:rustc-link-arg={}", res_path.display());
        }
        Ok(status) => println!(
            "cargo:warning=rc.exe 编译图标失败（exit {:?}），跳过内嵌",
            status.code()
        ),
        Err(e) => println!(
            "cargo:warning=找不到 rc.exe（{e}），跳过图标内嵌；\
             请在 VS 开发者环境或 aw-qtui 的 tools/vcenv.ps1 下构建"
        ),
    }
}

#[cfg(not(windows))]
fn embed_windows_icon() {}
