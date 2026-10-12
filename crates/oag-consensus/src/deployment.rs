//! ソフトフォークの有効化 (採掘者の印)。
//!
//! 採掘者がブロックヘッダの `version` に印を付け、ある区切りの中で印の
//! 付いたブロックが閾値に届いたら、新しい規則を有効にする。**BIP9 に
//! 倣う。** ビットコインとの違いは、期間を時刻ではなく高さで区切ること
//! だけである (BIP8 と同じ)。
//!
//! # 状態
//!
//! ```text
//!   Defined ──(開始の高さ)──▶ Started ──(閾値に届く)──▶ LockedIn ──▶ Active
//!                               │
//!                               └──(期限の高さ)──▶ Failed
//! ```
//!
//! 状態は**区切りの頭でしか変わらない。** ある区切りの状態は、1 つ前の
//! 区切りの状態と、その区切りで印の付いたブロックの数だけで決まる。
//! したがって、あるブロックで規則が効くかどうかは**祖先のブロックだけ**
//! から決まり、リオーグで枝が変われば答えも変わる。数えるのは `oag-chain`
//! の責務であり、ここは数えた結果から次の状態を決める規則だけを持つ。
//!
//! 参照: `docs/SPEC.md` §10.9

/// 印として数える版数の上位 3 ビットの位置 (BIP9)。
pub const VERSIONBITS_TOP_MASK: u32 = 0xE000_0000;

/// 印として数える版数の上位 3 ビットの値 (BIP9)。
///
/// 上位 3 ビットがこの形のときだけ、下のビットを印として読む。将来、版数の
/// 使い方を変えたくなったときに、この 3 ビットで区別できるようにしておく。
pub const VERSIONBITS_TOP_BITS: u32 = 0x2000_0000;

/// 1 つのソフトフォークの、印による有効化の決まり。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Deployment {
    /// 印に使うビット。**bit 0 は使えない** (AUX_POW に予約済み、SPEC §9.5)。
    pub bit: u8,
    /// 数え始める区切りの頭の高さ。この高さ以降の最初の区切りから
    /// `Started` になる。
    pub start_height: u64,
    /// 期限。この高さ以降に始まる区切りで、まだ閾値に届いていなければ
    /// `Failed` にする。
    pub timeout_height: u64,
    /// 1 つの区切りのブロック数。
    pub period: u64,
    /// 閾値。区切りの中で印の付いたブロックがこの数以上なら `LockedIn`。
    pub threshold: u64,
}

/// 有効化のしかた。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Activation {
    /// 最初から有効。testnet と regtest (過去のブロックが無効にならない
    /// うちに始めたため)。
    Always,
    /// 採掘者の印で有効にする。
    Signalled(Deployment),
    /// 有効にしない。
    Never,
}

/// 有効化の状態 (BIP9)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DeploymentState {
    /// まだ数え始めていない。
    Defined,
    /// 数えている。
    Started,
    /// 閾値に届いた。次の区切りから有効になる。**取り消せない。**
    LockedIn,
    /// 有効。**以後ずっと有効である。**
    Active,
    /// 期限までに届かなかった。**以後ずっとこのまま。**
    Failed,
}

impl DeploymentState {
    /// 新しい規則が効くか。
    pub fn is_active(self) -> bool {
        self == DeploymentState::Active
    }

    /// 採掘者がこの状態のブロックに印を付けるべきか。
    ///
    /// BIP9 と同じく、閾値に届いた後の猶予の区切りでも付け続ける。
    pub fn should_signal(self) -> bool {
        matches!(self, DeploymentState::Started | DeploymentState::LockedIn)
    }

    /// 表示用の名前。
    pub fn name(self) -> &'static str {
        match self {
            DeploymentState::Defined => "defined",
            DeploymentState::Started => "started",
            DeploymentState::LockedIn => "locked_in",
            DeploymentState::Active => "active",
            DeploymentState::Failed => "failed",
        }
    }
}

impl Deployment {
    /// 印の付いた版数 (上位 3 ビット + 印のビット)。
    pub const fn version_bits(&self) -> u32 {
        VERSIONBITS_TOP_BITS | (1u32 << self.bit)
    }

    /// その版数のブロックが、このソフトフォークに印を付けているか。
    pub const fn signals(&self, version: u32) -> bool {
        version & VERSIONBITS_TOP_MASK == VERSIONBITS_TOP_BITS && version & (1u32 << self.bit) != 0
    }

    /// 高さ `height` のブロックが属する区切りの頭の高さ。
    pub const fn period_start(&self, height: u64) -> u64 {
        height - height % self.period
    }

    /// 1 つ前の区切りの状態から、高さ `period_start` で始まる区切りの状態を
    /// 決める。
    ///
    /// `signals` は 1 つ前の区切りで印の付いたブロックの数である。
    /// 前が `Started` のときにしか見ない。
    ///
    /// ビットコイン (Bitcoin Core) と同じく、`Started` では**閾値を期限より
    /// 先に見る。** 期限の区切りで届いていれば有効になる。
    pub const fn next_state(
        &self,
        previous: DeploymentState,
        period_start: u64,
        signals: u64,
    ) -> DeploymentState {
        match previous {
            DeploymentState::Defined => {
                if period_start >= self.timeout_height {
                    DeploymentState::Failed
                } else if period_start >= self.start_height {
                    DeploymentState::Started
                } else {
                    DeploymentState::Defined
                }
            }
            DeploymentState::Started => {
                if signals >= self.threshold {
                    DeploymentState::LockedIn
                } else if period_start >= self.timeout_height {
                    DeploymentState::Failed
                } else {
                    DeploymentState::Started
                }
            }
            DeploymentState::LockedIn => DeploymentState::Active,
            DeploymentState::Active => DeploymentState::Active,
            DeploymentState::Failed => DeploymentState::Failed,
        }
    }
}

/// mainnet の区切りのブロック数 (約 1 日)。
pub const MAINNET_PERIOD: u64 = 1_440;

/// mainnet の閾値。区切りの 2/3 (`MAINNET_PERIOD * 2 / 3`)。
pub const MAINNET_THRESHOLD: u64 = 960;

/// コミットメント出力と、署名の末尾の `0x00` を無効にする規則
/// (`validate::Rules`) をまとめて有効にするソフトフォーク。
///
/// # mainnet
///
/// - 印は **bit 1**。bit 0 は AUX_POW に予約済みで、立っていると無効。
/// - **1440 ブロックごとの区切り**で、960 以上 (2/3) に印が付いたら
///   `LockedIn`、その次の区切りから `Active`。
/// - 数え始めは高さ **40,320** (= 28 × 1440)。相対 locktime の強制
///   (高さ 40,000) より後の、最初の区切りの頭である。ソフトフォークを
///   続けて重ねない (CLAUDE.md)。
/// - 期限は数え始めから 365 区切り (約 1 年) 後。届かなければ `Failed`。
///
/// testnet と regtest は最初から有効にする。
pub const fn commitment_activation(network: oag_primitives::Network) -> Activation {
    match network {
        oag_primitives::Network::Mainnet => Activation::Signalled(Deployment {
            bit: 1,
            start_height: 40_320,
            timeout_height: 40_320 + 365 * MAINNET_PERIOD,
            period: MAINNET_PERIOD,
            threshold: MAINNET_THRESHOLD,
        }),
        oag_primitives::Network::Testnet | oag_primitives::Network::Regtest => Activation::Always,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::VERSION_BIT_AUX_POW;
    use oag_primitives::Network;
    use DeploymentState::*;

    fn small() -> Deployment {
        Deployment {
            bit: 1,
            start_height: 20,
            timeout_height: 60,
            period: 10,
            threshold: 7,
        }
    }

    #[test]
    fn mainnet_parameters_are_what_was_decided() {
        let Activation::Signalled(d) = commitment_activation(Network::Mainnet) else {
            panic!("mainnet activates by signalling");
        };
        assert_eq!(d.bit, 1);
        assert_eq!(d.period, 1_440);
        assert_eq!(d.threshold, 960);
        assert_eq!(d.threshold * 3, d.period * 2, "two thirds");
        assert_eq!(d.start_height % d.period, 0, "starts at a period boundary");
        assert!(d.start_height > 40_000, "after the relative locktime fork");
        assert_eq!(d.timeout_height - d.start_height, 365 * d.period);
        // 印のビットが AUX_POW と重ならないこと。
        assert_eq!(d.version_bits() & VERSION_BIT_AUX_POW, 0);
        assert_eq!(d.version_bits(), 0x2000_0002);

        assert_eq!(commitment_activation(Network::Testnet), Activation::Always);
        assert_eq!(commitment_activation(Network::Regtest), Activation::Always);
    }

    #[test]
    fn only_the_bip9_top_bits_with_the_bit_count_as_a_signal() {
        let d = small();
        assert!(d.signals(0x2000_0002));
        assert!(d.signals(0x2000_0006), "other bits may be set too");
        assert!(!d.signals(0x0000_0002), "the top bits are required");
        assert!(!d.signals(0x6000_0002), "the top bits must be exactly 001");
        assert!(!d.signals(0x2000_0000), "the bit itself is required");
        assert!(!d.signals(0), "today's blocks do not signal");
    }

    #[test]
    fn the_period_start_is_a_multiple_of_the_period() {
        let d = small();
        assert_eq!(d.period_start(0), 0);
        assert_eq!(d.period_start(9), 0);
        assert_eq!(d.period_start(10), 10);
        assert_eq!(d.period_start(29), 20);
    }

    #[test]
    fn defined_becomes_started_at_the_start_height() {
        let d = small();
        assert_eq!(d.next_state(Defined, 10, 10), Defined);
        assert_eq!(d.next_state(Defined, 20, 0), Started);
    }

    #[test]
    fn started_locks_in_at_the_threshold_and_not_below() {
        let d = small();
        assert_eq!(d.next_state(Started, 30, 6), Started);
        assert_eq!(d.next_state(Started, 30, 7), LockedIn);
        assert_eq!(d.next_state(Started, 30, 10), LockedIn);
    }

    #[test]
    fn locked_in_always_becomes_active_and_stays() {
        let d = small();
        // 猶予の区切りで印が減っても取り消さない。
        assert_eq!(d.next_state(LockedIn, 40, 0), Active);
        assert_eq!(d.next_state(Active, 50, 0), Active);
        assert_eq!(d.next_state(Active, 1_000, 0), Active);
    }

    #[test]
    fn the_threshold_is_checked_before_the_timeout() {
        let d = small();
        assert_eq!(d.next_state(Started, 60, 7), LockedIn);
        assert_eq!(d.next_state(Started, 60, 6), Failed);
        assert_eq!(d.next_state(Failed, 70, 10), Failed);
    }

    #[test]
    fn a_deployment_that_never_started_fails_at_the_timeout() {
        let d = Deployment {
            start_height: 100,
            timeout_height: 50,
            ..small()
        };
        assert_eq!(d.next_state(Defined, 50, 0), Failed);
    }

    #[test]
    fn signalling_continues_through_lock_in() {
        assert!(!Defined.should_signal());
        assert!(Started.should_signal());
        assert!(LockedIn.should_signal());
        assert!(!Active.should_signal());
        assert!(!Failed.should_signal());
        assert!(Active.is_active());
        assert!(!LockedIn.is_active());
    }
}
