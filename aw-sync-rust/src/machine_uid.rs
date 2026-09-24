//! 装机指纹（machine_uid）：认出「重装之后的还是那台机器」。
//!
//! 背景：`device_id` 是 `<data_dir>/device_id` 里的一个随机 UUID，卸载重装 / 清除应用
//! 数据 / 换签名键都会让它换值。每换一次，信任列表就多一行幽灵，而列表里那些行本来就
//! 无从分辨「哪台是刚重装的平板」。mDNS 只解决「怎么互相找到」，解决不了「谁是谁」。
//!
//! 于是引入一个**与安装实例无关**的标识：从 OS 级稳定值派生
//! （Windows `HKLM\SOFTWARE\Microsoft\Cryptography\MachineGuid`、Linux `/etc/machine-id`、
//! Android 由 Kotlin 读 `Settings.Secure.ANDROID_ID` 后注入）。
//!
//! 两条纪律，都写在类型与调用点上，不靠自觉：
//!
//! 1. **它不是凭据。** 那些 OS 值本机任何进程都能读、能仿冒，所以它只能驱动
//!    「UI 上提示这两行疑似同一台机器 + 归并显示与历史归因」，**绝不**参与密钥信任，
//!    也绝不省掉配对时的安全码比对。
//! 2. **它不能被动播出去。** 那是跨网络可跟踪的机器标识。只随用户主动发起的配对
//!    HTTP 报文走一次；UDP 广播与 mDNS TXT 一律抹掉（见 `discovery` 的 redaction）。
//!
//! 已知会退化（都往「多一行」退，不会错并）：换签名键（debug↔release）、恢复出厂、
//! Android 工作资料 → uid 变。已知假阳性：同一台物理机上跑两个独立实例 → uid 相同，
//! 所以归并必须由用户点确认，系统绝不自动折叠。

use std::sync::{Mutex, OnceLock};

use sha2::{Digest, Sha256};

/// 派生前缀：换算法/换口径时 bump 版本号，让新旧 uid 必然不同，而不是撞在一起。
const UID_PREFIX: &str = "aw-muid/1";

/// 展示与存储长度：16 个 hex（64 bit）—— 够避免真实碰撞，又短到能整串显示出来比对。
const UID_HEX_LEN: usize = 16;

/// 宿主注入的原始值：`(platform, raw)`。
///
/// Android 的 ANDROID_ID 只有 Java 侧读得到，而派生算法只应有一份，所以注入的是
/// **原始值 + 平台标签**，哈希统一留在 Rust。
static INJECTED: OnceLock<Mutex<Option<(String, String)>>> = OnceLock::new();

/// 由 OS 级原始值派生指纹。纯函数，单测直接盯它。
pub fn derive(platform: &str, raw: &str) -> Option<String> {
    let raw = raw.trim();
    if platform.is_empty() || raw.is_empty() {
        return None;
    }
    let digest = Sha256::digest(format!("{UID_PREFIX}\n{platform}\n{raw}").as_bytes());
    let hex = digest
        .iter()
        .take(UID_HEX_LEN / 2)
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    Some(hex)
}

/// 宿主（Android JNI）注入原始标识。桌面端不需要——`native_raw()` 自己能读到。
pub fn set_raw(platform: &str, raw: &str) {
    let slot = INJECTED.get_or_init(|| Mutex::new(None));
    if let Ok(mut g) = slot.lock() {
        *g = Some((platform.to_string(), raw.to_string()));
    }
}

/// 注入的原始标识（仅测试与调试用：清掉注入值以观察原生探测分支）。
pub fn clear_injected() {
    if let Some(slot) = INJECTED.get() {
        if let Ok(mut g) = slot.lock() {
            *g = None;
        }
    }
}

/// 本机装机指纹；读不到稳定标识就返回 None（宁可退回「按 device_id 各算一台」。
pub fn machine_uid() -> Option<String> {
    if let Some(slot) = INJECTED.get() {
        if let Ok(g) = slot.lock() {
            if let Some((p, raw)) = g.as_ref() {
                return derive(p, raw);
            }
        }
    }
    let (platform, raw) = native_raw()?;
    derive(platform.as_str(), raw.as_str())
}

/// 各平台的 OS 级原始标识。
#[cfg(target_os = "windows")]
fn native_raw() -> Option<(String, String)> {
    machine_guid().map(|guid| ("windows".to_string(), guid))
}

#[cfg(all(target_os = "linux", not(target_os = "android")))]
fn native_raw() -> Option<(String, String)> {
    // 两份是同一个值的不同落点：容器里常只有 /etc/machine-id，桌面发行版两份都有。
    for path in ["/etc/machine-id", "/var/lib/dbus/machine-id"] {
        if let Ok(s) = std::fs::read_to_string(path) {
            let s = s.trim().to_string();
            if !s.is_empty() {
                return Some(("linux".to_string(), s));
            }
        }
    }
    None
}

/// Android 侧只能由 Kotlin 注入（见 `RustInterface.setSyncMachineUid`）。
#[cfg(target_os = "android")]
fn native_raw() -> Option<(String, String)> {
    None
}

/// macOS 目前没有目标设备，留着显式失败而不是假装能读。
#[cfg(target_os = "macos")]
fn native_raw() -> Option<(String, String)> {
    None
}

/// 读 `HKLM\SOFTWARE\Microsoft\Cryptography` 的 `MachineGuid`。
///
/// 必须带 `KEY_WOW64_64KEY`：32 位进程读注册表会被重定向到 `WOW6432Node`，
/// 而那里的是一个**复制品**，与真值不同——那样同一台机器会派生出两个指纹。
#[cfg(target_os = "windows")]
fn machine_guid() -> Option<String> {
    use std::ffi::c_void;
    use windows_sys::Win32::Foundation::ERROR_SUCCESS;
    use windows_sys::Win32::System::Registry::{
        RegCloseKey, RegGetValueW, RegOpenKeyExW, HKEY, HKEY_LOCAL_MACHINE, KEY_READ,
        KEY_WOW64_64KEY, RRF_RT_REG_SZ,
    };

    unsafe {
        let subkey: Vec<u16> = "SOFTWARE\\Microsoft\\Cryptography\0"
            .encode_utf16()
            .collect();
        let value: Vec<u16> = "MachineGuid\0".encode_utf16().collect();

        // windows-sys 里 HKEY 就是裸 isize（0 = NULL），不是指针也不是句柄包装结构体。
        let mut hklm64: HKEY = 0;
        let open = RegOpenKeyExW(
            HKEY_LOCAL_MACHINE,
            subkey.as_ptr(),
            0,
            KEY_READ | KEY_WOW64_64KEY,
            &mut hklm64,
        );
        if open != ERROR_SUCCESS {
            return None;
        }

        // RegGetValueW 负责处理类型与结尾 NUL；先传 null 问长度，再按长度取。
        let mut cch: u32 = 0;
        let ask = RegGetValueW(
            hklm64,
            std::ptr::null(),
            value.as_ptr(),
            RRF_RT_REG_SZ,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            &mut cch,
        );
        let mut out: Vec<u16> = Vec::new();
        if ask == ERROR_SUCCESS && cch >= 2 {
            out = vec![0u16; (cch as usize) / 2];
            let get = RegGetValueW(
                hklm64,
                std::ptr::null(),
                value.as_ptr(),
                RRF_RT_REG_SZ,
                std::ptr::null_mut(),
                out.as_mut_ptr() as *mut c_void,
                &mut cch,
            );
            if get != ERROR_SUCCESS {
                out.clear();
            }
        }
        RegCloseKey(hklm64);

        let guid: String = String::from_utf16_lossy(&out);
        let guid = guid.trim().trim_end_matches('\0').to_string();
        if guid.is_empty() {
            None
        } else {
            Some(guid)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derive_is_stable_platform_scoped_and_short() {
        let a = derive("windows", "11111111-2222-3333-4444-555555555555").unwrap();
        let b = derive("windows", "11111111-2222-3333-4444-555555555555").unwrap();
        assert_eq!(a, b, "同一原始值必须得到同一指纹，否则归并无从谈起");
        assert_eq!(a.len(), UID_HEX_LEN);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));

        // 平台命名空间：不同平台上同一个字符串不该撞车
        let c = derive("android", "11111111-2222-3333-4444-555555555555").unwrap();
        assert_ne!(a, c);
    }

    #[test]
    fn empty_raw_yields_no_identity() {
        assert_eq!(derive("windows", ""), None);
        assert_eq!(derive("windows", "   "), None);
        assert_eq!(derive("", "abc"), None);
    }

    #[test]
    fn injected_value_overrides_native_probe() {
        // 全局注入位在同一进程内跨测试共享，所以这条测试自己负责收尾。
        set_raw("android", "injected-test-value");
        let got = machine_uid().expect("注入后必须能拿到指纹");
        assert_eq!(got, derive("android", "injected-test-value").unwrap());
        clear_injected();
    }
}
