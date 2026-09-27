//! ノードの記録。
//!
//! # なぜ印を付けるのか
//!
//! **「運んでいる」と「確かめた」は別の話である。** 混ぜて出すと、
//! 止まったときにどちらで止まったのかが読めない。相手が寄越さないのか、
//! こちらが捌けていないのかは、対処が正反対になる。
//!
//! 行の先頭に何の話かを置く。目で追えるし、`grep '\[sync\]'` で片方だけ
//! 取り出せる。
//!
//! ```text
//! [peer] connected to 203.0.113.9:9444 (/oag-node:0.1.0/, height 2126)
//! [sync] headers +2000 (2000 of them)  known up to height 2000
//! [sync] bodies 1204/2126 (57%)  12.4 blk/s  922 to go
//! [sync] caught up  height 2126
//! [check] connected height 2127  1 tx  203 B  mempool 0
//! [check] reorg  -1 +2  height 2129
//! [check]   dropped 2128  9c41d07e
//! [check]   adopted 2128  5b2e88a1
//! [check]   adopted 2129  e07f3c52
//! [tx] received 3f2a1b9c  fee 0.0001 OAG  186 B  mempool 1
//! ```
//!
//! # 出し先
//!
//! 進んでいることの記録は標準出力、警告は標準エラーに出す。`2>` で
//! 分ければ、困ったことだけを別に残せる。

use oag_primitives::Hash;

/// ハッシュの頭 8 桁。
///
/// 64 桁を毎行並べると、肝心の数字が画面の外へ出る。取り違えの心配が
/// ある場面 (掘れたブロックなど) では全部出す。
pub fn short(hash: &Hash) -> String {
    let full = hash.to_string();
    full.chars().take(8).collect()
}

/// リオーグで入れ替わったブロックを 1 行ずつ。
///
/// **件数だけでは何が起きたか分からない。** 同時に 2 つ掘れたのか、
/// 長く別れていたのかは、どの高さの何が入れ替わったかを見て初めて分かる。
/// ハッシュがあればエクスプローラで引ける。深いリオーグで画面が埋まら
/// ないよう、片側 [`REORG_LINES`] 行までにする。
///
/// `height` は新しい先端の高さ。取り消した側は高い順、採用した側は低い順に
/// 並べる (どちらも [`oag_chain::Reorg`] の並びのまま)。
pub fn reorg_detail(reorg: &oag_chain::Reorg, height: u64) -> Vec<String> {
    let fork = height.saturating_sub(reorg.connected.len() as u64);
    let mut lines = Vec::new();
    let mut side = |word: &str, hashes: &[Hash], height_of: &dyn Fn(usize) -> u64| {
        for (i, hash) in hashes.iter().take(REORG_LINES).enumerate() {
            lines.push(format!("  {word} {}  {}", height_of(i), short(hash)));
        }
        if hashes.len() > REORG_LINES {
            lines.push(format!("  {word} … {} more", hashes.len() - REORG_LINES));
        }
    };
    let dropped = reorg.disconnected.len() as u64;
    side("dropped", &reorg.disconnected, &|i| {
        fork + dropped - i as u64
    });
    side("adopted", &reorg.connected, &|i| fork + 1 + i as u64);
    lines
}

/// [`reorg_detail`] が片側に出す行数の上限。
pub const REORG_LINES: usize = 5;

/// 大きさの表記。
pub fn bytes(n: usize) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    match n {
        n if n < 1024 => format!("{n} B"),
        n if (n as f64) < MIB => format!("{:.1} KiB", n as f64 / KIB),
        n => format!("{:.1} MiB", n as f64 / MIB),
    }
}

/// 同期 — 相手から運んでくる話。
///
/// ヘッダと本体がどこまで来たか。**中身が正しいかはここでは言わない。**
#[macro_export]
macro_rules! log_sync {
    ($($arg:tt)*) => { println!("[sync] {}", format_args!($($arg)*)) };
}

/// 検証 — 運んできたものを自分で確かめた話。
///
/// 接続できた、リオーグした。**自分が納得したことだけをここに出す。**
#[macro_export]
macro_rules! log_verify {
    ($($arg:tt)*) => { println!("[check] {}", format_args!($($arg)*)) };
}

/// 取引 — mempool の出入り。
#[macro_export]
macro_rules! log_tx {
    ($($arg:tt)*) => { println!("[tx] {}", format_args!($($arg)*)) };
}

/// ピア — 接続の出入り。
#[macro_export]
macro_rules! log_peer {
    ($($arg:tt)*) => { println!("[peer] {}", format_args!($($arg)*)) };
}

/// 採掘。
#[macro_export]
macro_rules! log_mine {
    ($($arg:tt)*) => { println!("[mining] {}", format_args!($($arg)*)) };
}

/// 警告。標準エラーへ出す。
#[macro_export]
macro_rules! log_warn {
    ($($arg:tt)*) => { eprintln!("[warn] {}", format_args!($($arg)*)) };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_short_hash_is_eight_digits() {
        assert_eq!(short(&Hash::ZERO).len(), 8);
        assert_eq!(short(&Hash::ZERO), "00000000");
    }

    fn hash(n: u8) -> Hash {
        oag_primitives::hash::txid(&[n])
    }

    #[test]
    fn a_reorg_lists_what_was_dropped_and_what_was_adopted_with_heights() {
        // 2128 で別れ、手元の 2128 を捨てて向こうの 2128, 2129 を採った。
        let reorg = oag_chain::Reorg {
            disconnected: vec![hash(1)],
            connected: vec![hash(2), hash(3)],
        };
        let lines = reorg_detail(&reorg, 2129);
        assert_eq!(
            lines,
            vec![
                format!("  dropped 2128  {}", short(&hash(1))),
                format!("  adopted 2128  {}", short(&hash(2))),
                format!("  adopted 2129  {}", short(&hash(3))),
            ]
        );
    }

    #[test]
    fn dropped_blocks_are_listed_from_the_old_tip_down() {
        let reorg = oag_chain::Reorg {
            disconnected: vec![hash(1), hash(2)],
            connected: vec![hash(3), hash(4), hash(5)],
        };
        let lines = reorg_detail(&reorg, 13);
        assert!(lines[0].starts_with("  dropped 12 "), "{lines:?}");
        assert!(lines[1].starts_with("  dropped 11 "), "{lines:?}");
        assert!(lines[2].starts_with("  adopted 11 "), "{lines:?}");
        assert!(lines[4].starts_with("  adopted 13 "), "{lines:?}");
    }

    #[test]
    fn a_deep_reorg_is_cut_short() {
        let many: Vec<Hash> = (0..20).map(hash).collect();
        let reorg = oag_chain::Reorg {
            disconnected: many.clone(),
            connected: many,
        };
        let lines = reorg_detail(&reorg, 100);
        assert_eq!(lines.len(), 2 * (REORG_LINES + 1));
        assert_eq!(lines[REORG_LINES], "  dropped … 15 more");
    }

    #[test]
    fn sizes_change_unit_at_the_boundary() {
        assert_eq!(bytes(0), "0 B");
        assert_eq!(bytes(1023), "1023 B");
        assert_eq!(bytes(1024), "1.0 KiB");
        assert_eq!(bytes(1024 * 1024), "1.0 MiB");
    }
}
