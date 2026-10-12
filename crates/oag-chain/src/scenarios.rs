//! 記憶域によらず走らせる試験手順。
//!
//! リオーグは最もバグりやすい箇所であり、記憶域ごとに別のテストを書くと
//! 片方でしか通らない実装ができてしまう。ここに手順を一本化し、
//! メモリ実装と永続化実装の双方に対して同じものを走らせる。
//!

use crate::chain::{AcceptOutcome, Chain, ChainError, HeaderOutcome, Retarget};
use crate::genesis::GenesisSpec;
use crate::index::BlockStatus;
use crate::store::ChainStore;
use oag_consensus::deployment::{Activation, Deployment, DeploymentState};
use oag_consensus::lock::Lock;
use oag_consensus::sighash::{sighash, SighashType};
use oag_consensus::tx::{
    encode_coinbase_signature, OutPoint, TxInput, CURRENT_TX_VERSION, SEQUENCE_TYPE_FLAG,
};
use oag_consensus::utxo::UtxoView;
use oag_consensus::validate::{AcceptAnyPow, Rules};
use oag_consensus::{Block, BlockHeader, Transaction, TxOutput};
use oag_primitives::{merkle, Amount, Hash, Network, SecretKey};

/// 試験で用いる「現在時刻」。
pub const NOW: i64 = 3_000_000_000;
/// ジェネシスのタイムスタンプ。
pub const GENESIS_TIME: i64 = 1_800_000_000;
/// 試験で用いる難易度。
pub const DIFFICULTY: u64 = 1;

/// 試験用のジェネシスブロック。
pub fn genesis() -> Block {
    GenesisSpec::without_reward(Network::Regtest, GENESIS_TIME, b"Orange regtest").build(0)
}

/// 記憶域からチェーンを起こす。
///
/// 難易度調整は**有効**にする。regtest のジェネシスを使っているが、
/// 止めてしまうと調整まわりを試験できなくなる。止めた場合の振る舞いは
/// [`the_difficulty_never_moves_without_retargeting`] で別に確かめる。
pub fn open<S: ChainStore>(store: S) -> Chain<S> {
    Chain::open(store, genesis(), DIFFICULTY, Retarget::Enabled).expect("genesis is valid")
}

/// インデックスの 1 件を引く。**知っていることを前提にする。**
pub fn entry_of<S: ChainStore>(chain: &Chain<S>, hash: &Hash) -> crate::index::BlockIndexEntry {
    chain
        .entry(hash)
        .expect("storage is readable")
        .expect("that block is known")
}

/// `parent` の上に載る有効なブロックを組み立てる。
///
/// `salt` を変えるとコインベースが変わり、同じ高さの別のブロックになる。
pub fn build_on<S: ChainStore>(chain: &Chain<S>, parent: Hash, salt: u64) -> Block {
    let parent_entry = entry_of(chain, &parent);
    let height = parent_entry.height() + 1;

    let mut input = TxInput::new(OutPoint::null());
    input.signature = encode_coinbase_signature(height, &salt.to_le_bytes());
    let coinbase = Transaction {
        version: CURRENT_TX_VERSION,
        inputs: vec![input],
        outputs: vec![TxOutput::new(
            oag_consensus::params::block_subsidy(height),
            Lock::pay_to_pubkey(&SecretKey::generate().public_key()),
        )],
        locktime: 0,
    };
    let merkle_root = merkle::merkle_root(&[coinbase.txid()]).unwrap();

    Block {
        header: BlockHeader {
            version: 0,
            prev_hash: parent,
            merkle_root,
            timestamp: GENESIS_TIME + height as i64 * 60,
            difficulty: chain
                .expected_difficulty_for_child_of(&parent)
                .expect("difficulty can be computed"),
            height,
            nonce: salt,
        },
        transactions: vec![coinbase],
    }
}

/// `parent` の上に `count` 個のブロックを積む。
pub fn extend<S: ChainStore>(
    chain: &mut Chain<S>,
    mut parent: Hash,
    count: usize,
    salt: u64,
) -> Vec<Hash> {
    let mut hashes = Vec::with_capacity(count);
    for i in 0..count {
        let block = build_on(chain, parent, salt * 1_000_000 + i as u64);
        parent = block.header.hash();
        chain
            .accept_block(block, &AcceptAnyPow, NOW)
            .expect("a valid block");
        hashes.push(parent);
    }
    hashes
}

/// コインベース出力が UTXO セットに存在するか。
fn has_coinbase_output<S: ChainStore>(chain: &Chain<S>, block_hash: &Hash) -> bool {
    let block = chain
        .store()
        .block(block_hash)
        .ok()
        .flatten()
        .expect("the body is present");
    let outpoint = OutPoint::new(block.transactions[0].txid(), 0);
    chain
        .utxo_view()
        .expect("a view can be taken")
        .get(&outpoint)
        .expect("readable")
        .is_some()
}

// ━━━━━━━━ シナリオ ━━━━━━━━

/// 親から子への線を、記憶域が持っていること。
///
/// これはインデックスではなく**記憶域**が持つ。無効の印を子孫へ広げる
/// ときにしか引かず、メモリに持つと高さに比例して伸びるためである
/// (`docs/SPEC.md` §19)。
pub fn children_come_from_the_store<S: ChainStore>(store: S) {
    let mut chain = open(store);
    let genesis_hash = genesis().header.hash();
    let main = extend(&mut chain, genesis_hash, 3, 1);

    // 高さ 1 の上に、もう 1 本生やす。
    let fork = build_on(&chain, main[0], 777);
    let fork_hash = fork.header.hash();
    chain.accept_block(fork, &AcceptAnyPow, NOW).expect("valid");

    let mut children = chain
        .store()
        .children_of(&main[0])
        .expect("can be looked up");
    children.sort_unstable();
    let mut expected = vec![main[1], fork_hash];
    expected.sort_unstable();
    assert_eq!(children, expected, "not both children could be looked up");

    // ジェネシスは親を持たないので、0 のハッシュに子を登録しない。
    assert!(chain
        .store()
        .children_of(&Hash::ZERO)
        .expect("can be looked up")
        .is_empty());
    // 先端には子がいない。
    assert!(chain
        .store()
        .children_of(&main[2])
        .expect("can be looked up")
        .is_empty());

    // 同じブロックの状態が何度書き直されても、子は二重にならない。
    let entry = entry_of(&chain, &main[1]);
    chain.store().put_index_entry(&entry).expect("writable");
    chain.store().put_index_entry(&entry).expect("writable");
    assert_eq!(
        chain
            .store()
            .children_of(&main[0])
            .expect("can be looked up")
            .len(),
        2,
        "a child is registered twice"
    );
}

/// ジェネシスから始まること。
pub fn starts_at_genesis<S: ChainStore>(store: S) {
    let chain = open(store);
    assert_eq!(chain.height().unwrap(), 0);
    assert_eq!(chain.tip().unwrap().hash, genesis().header.hash());
    assert_eq!(chain.tip().unwrap().cumulative_work, u128::from(DIFFICULTY));
    assert_eq!(chain.indexed_blocks().unwrap(), 1);
    assert_eq!(
        chain.store().utxo_count().unwrap(),
        0,
        "genesis forgoes the reward"
    );
}

/// 先端を伸ばせること。
pub fn extends_the_tip<S: ChainStore>(store: S) {
    let mut chain = open(store);
    let tip = chain.tip().unwrap().hash;
    let block = build_on(&chain, tip, 1);
    let hash = block.header.hash();

    assert_eq!(
        chain.accept_block(block, &AcceptAnyPow, NOW).unwrap(),
        AcceptOutcome::ExtendedTip
    );
    assert_eq!(chain.height().unwrap(), 1);
    assert_eq!(chain.tip().unwrap().hash, hash);
    assert_eq!(chain.hash_at_height(1).unwrap(), Some(hash));
    assert_eq!(chain.store().utxo_count().unwrap(), 1);
}

/// 同じブロックを二度受け取っても重複として扱うこと。
pub fn rejects_duplicates<S: ChainStore>(store: S) {
    let mut chain = open(store);
    let tip = chain.tip().unwrap().hash;
    let block = build_on(&chain, tip, 1);
    chain
        .accept_block(block.clone(), &AcceptAnyPow, NOW)
        .unwrap();
    assert_eq!(
        chain.accept_block(block, &AcceptAnyPow, NOW).unwrap(),
        AcceptOutcome::Duplicate
    );
    assert_eq!(chain.height().unwrap(), 1);
}

/// 親を知らないブロックを拒否すること。
pub fn rejects_orphans<S: ChainStore>(store: S) {
    let mut chain = open(store);
    let tip = chain.tip().unwrap().hash;
    let mut block = build_on(&chain, tip, 1);
    block.header.prev_hash = oag_primitives::hash::block_hash(b"unknown");
    assert!(matches!(
        chain.accept_block(block, &AcceptAnyPow, NOW),
        Err(ChainError::UnknownParent(_))
    ));
}

/// 作業量が足りない枝はサイドチェーンにとどまること。
pub fn shorter_branch_stays_a_side_chain<S: ChainStore>(store: S) {
    let mut chain = open(store);
    let fork = chain.tip().unwrap().hash;
    let main = extend(&mut chain, fork, 3, 1);

    let side = build_on(&chain, fork, 999);
    assert_eq!(
        chain.accept_block(side, &AcceptAnyPow, NOW).unwrap(),
        AcceptOutcome::SideChain
    );
    assert_eq!(chain.tip().unwrap().hash, *main.last().unwrap());
    assert_eq!(chain.height().unwrap(), 3);
    assert_eq!(
        chain.indexed_blocks().unwrap(),
        5,
        "side chains are recorded too"
    );
}

/// 作業量で上回る枝が現れたらリオーグすること。同点では切り替えないこと。
pub fn a_heavier_branch_triggers_a_reorg<S: ChainStore>(store: S) {
    let mut chain = open(store);
    let fork = chain.tip().unwrap().hash;
    let main = extend(&mut chain, fork, 3, 1);
    assert_eq!(chain.tip().unwrap().hash, *main.last().unwrap());

    let mut parent = fork;
    let mut side = Vec::new();
    for i in 0..4 {
        let block = build_on(&chain, parent, 500 + i);
        parent = block.header.hash();
        side.push(parent);
        let outcome = chain.accept_block(block, &AcceptAnyPow, NOW).unwrap();
        if i < 3 {
            assert_eq!(outcome, AcceptOutcome::SideChain, "switched at branch {i}");
        } else {
            match outcome {
                AcceptOutcome::Reorganized(reorg) => {
                    assert_eq!(reorg.depth(), 3, "three blocks should be undone");
                    assert_eq!(reorg.connected.len(), 4);
                }
                other => panic!("it did not reorg: {other:?}"),
            }
        }
    }

    assert_eq!(chain.height().unwrap(), 4);
    assert_eq!(chain.tip().unwrap().hash, *side.last().unwrap());
    for (h, hash) in side.iter().enumerate() {
        assert_eq!(chain.hash_at_height(h as u64 + 1).unwrap(), Some(*hash));
    }

    // 取り消された枝のコインベース出力が消えていること。
    for hash in &main {
        assert!(
            !has_coinbase_output(&chain, hash),
            "outputs from the undone branch are still present"
        );
    }
    for hash in &side {
        assert!(has_coinbase_output(&chain, hash));
    }
}

/// リオーグ後の状態が、その枝を最初から積んだ場合と一致すること。
///
/// これが崩れると、リオーグを経験したノードだけが別の帳簿を持つ。
pub fn a_reorg_matches_a_direct_build<S: ChainStore>(forked_store: S, direct_store: S) {
    let mut forked = open(forked_store);
    let fork = forked.tip().unwrap().hash;
    extend(&mut forked, fork, 2, 1); // 捨てられる枝

    let mut parent = fork;
    let mut winner = Vec::new();
    for i in 0..5 {
        let block = build_on(&forked, parent, 700 + i);
        parent = block.header.hash();
        winner.push(block.clone());
        forked.accept_block(block, &AcceptAnyPow, NOW).unwrap();
    }

    let mut direct = open(direct_store);
    for block in &winner {
        direct
            .accept_block(block.clone(), &AcceptAnyPow, NOW)
            .unwrap();
    }

    assert_eq!(forked.tip().unwrap().hash, direct.tip().unwrap().hash);
    assert_eq!(forked.height().unwrap(), direct.height().unwrap());
    assert_eq!(
        forked.store().utxo_count().unwrap(),
        direct.store().utxo_count().unwrap(),
        "the post-reorg UTXO count does not match a directly built one"
    );

    let forked_view = forked.utxo_view().unwrap();
    let direct_view = direct.utxo_view().unwrap();
    for block in &winner {
        let outpoint = OutPoint::new(block.transactions[0].txid(), 0);
        assert_eq!(
            forked_view.get(&outpoint).unwrap(),
            direct_view.get(&outpoint).unwrap(),
            "the outputs at height {} disagreed",
            block.header.height
        );
    }
}

/// リオーグの途中で無効が判明しても、元のチェーンに戻ること。
pub fn an_invalid_block_in_a_heavier_branch_is_contained<S: ChainStore>(store: S) {
    let mut chain = open(store);
    let fork = chain.tip().unwrap().hash;
    let main = extend(&mut chain, fork, 2, 1);
    let good_tip = *main.last().unwrap();
    let good_count = chain.store().utxo_count().unwrap();

    let b1 = build_on(&chain, fork, 800);
    let b1_hash = b1.header.hash();
    chain.accept_block(b1, &AcceptAnyPow, NOW).unwrap();

    // コインベースを過大にする。ヘッダは正しいので接続時に初めて弾かれる。
    let mut b2 = build_on(&chain, b1_hash, 801);
    b2.transactions[0].outputs[0].amount = Amount::from_oag(1_000).unwrap();
    b2.header.merkle_root = merkle::merkle_root(&[b2.transactions[0].txid()]).unwrap();
    let b2_hash = b2.header.hash();
    chain.accept_block(b2, &AcceptAnyPow, NOW).unwrap();

    let b3 = build_on(&chain, b2_hash, 802);
    let b3_hash = b3.header.hash();
    let outcome = chain.accept_block(b3, &AcceptAnyPow, NOW).unwrap();

    assert_eq!(outcome, AcceptOutcome::SideChain);
    assert_eq!(
        chain.tip().unwrap().hash,
        good_tip,
        "it did not return to the original tip"
    );
    assert_eq!(chain.height().unwrap(), 2);
    assert_eq!(
        chain.store().utxo_count().unwrap(),
        good_count,
        "the UTXO set did not return to its original state"
    );
    for hash in &main {
        assert!(
            has_coinbase_output(&chain, hash),
            "outputs of the original branch were not restored"
        );
    }

    // 実際に失敗した b2 とその子孫にのみ印が付くこと。
    assert_eq!(
        entry_of(&chain, &b1_hash).status,
        BlockStatus::FullyValid,
        "invalidated as collateral damage"
    );
    assert_eq!(entry_of(&chain, &b2_hash).status, BlockStatus::Invalid);
    assert_eq!(entry_of(&chain, &b3_hash).status, BlockStatus::Invalid);
}

/// 無効な祖先を持つブロックが到着時に拒否されること。
pub fn children_of_an_invalid_block_are_rejected<S: ChainStore>(store: S) {
    let mut chain = open(store);
    let fork = chain.tip().unwrap().hash;
    extend(&mut chain, fork, 3, 1);

    let b1 = build_on(&chain, fork, 900);
    let b1_hash = b1.header.hash();
    chain.accept_block(b1, &AcceptAnyPow, NOW).unwrap();

    let mut b2 = build_on(&chain, b1_hash, 901);
    b2.transactions[0].outputs[0].amount = Amount::from_oag(1_000).unwrap();
    b2.header.merkle_root = merkle::merkle_root(&[b2.transactions[0].txid()]).unwrap();
    let b2_hash = b2.header.hash();
    chain.accept_block(b2, &AcceptAnyPow, NOW).unwrap();

    let b3 = build_on(&chain, b2_hash, 902);
    let b3_hash = b3.header.hash();
    chain.accept_block(b3, &AcceptAnyPow, NOW).unwrap();
    let b4 = build_on(&chain, b3_hash, 903);
    chain.accept_block(b4, &AcceptAnyPow, NOW).unwrap();

    assert_eq!(entry_of(&chain, &b2_hash).status, BlockStatus::Invalid);
    let b5 = build_on(&chain, b3_hash, 904);
    assert!(matches!(
        chain.accept_block(b5, &AcceptAnyPow, NOW),
        Err(ChainError::InvalidAncestor(_))
    ));
}

/// 深いリオーグでも状態が一致すること。
pub fn a_deep_reorg_stays_consistent<S: ChainStore>(forked_store: S, direct_store: S) {
    let mut chain = open(forked_store);
    let fork = chain.tip().unwrap().hash;
    extend(&mut chain, fork, 30, 1);
    assert_eq!(chain.height().unwrap(), 30);

    let mut parent = fork;
    let mut winner = Vec::new();
    for i in 0..31 {
        let block = build_on(&chain, parent, 2_000 + i);
        parent = block.header.hash();
        winner.push(block.clone());
        chain.accept_block(block, &AcceptAnyPow, NOW).unwrap();
    }

    assert_eq!(chain.height().unwrap(), 31);
    assert_eq!(
        chain.store().utxo_count().unwrap(),
        31,
        "only the winning branch's coinbase"
    );

    let mut direct = open(direct_store);
    for block in &winner {
        direct
            .accept_block(block.clone(), &AcceptAnyPow, NOW)
            .unwrap();
    }
    assert_eq!(
        chain.store().utxo_count().unwrap(),
        direct.store().utxo_count().unwrap()
    );
    assert_eq!(chain.tip().unwrap().hash, direct.tip().unwrap().hash);
}

/// 難易度が窓幅に達するまで固定であること。
pub fn the_difficulty_is_fixed_until_the_window_is_full<S: ChainStore>(store: S) {
    let mut chain = open(store);
    let tip = chain.tip().unwrap().hash;
    assert_eq!(
        chain.expected_difficulty_for_child_of(&tip).unwrap(),
        DIFFICULTY
    );

    let window = oag_pow::lwma::WINDOW as u64;
    extend(&mut chain, tip, window as usize, 1);
    assert_eq!(chain.height().unwrap(), window);

    let tip = chain.tip().unwrap().hash;
    assert_eq!(
        chain.expected_difficulty_for_child_of(&tip).unwrap(),
        DIFFICULTY,
        "at even intervals the difficulty does not change"
    );
}

/// Median Time Past がチェーンに追随すること。
pub fn median_time_past_follows_the_chain<S: ChainStore>(store: S) {
    let mut chain = open(store);
    let tip = chain.tip().unwrap().hash;
    assert_eq!(
        chain.median_time_past_for_child_of(&tip).unwrap(),
        GENESIS_TIME
    );

    extend(&mut chain, tip, 20, 1);
    let tip = chain.tip().unwrap().hash;
    // 直近 11 ブロック (高さ 10〜20) の中央値は高さ 15 のもの。
    assert_eq!(
        chain.median_time_past_for_child_of(&tip).unwrap(),
        GENESIS_TIME + 15 * 60
    );
}

// ━━━━━━━━ headers-first 同期 ━━━━━━━━

/// ヘッダだけを受け取っても先端は動かないこと。
///
/// 本体が無いのだから接続できない。ここで先端が動いてしまうと、
/// 中身を検証していないブロックを採用したことになる。
pub fn headers_alone_do_not_move_the_tip<S: ChainStore>(store: S) {
    let mut chain = open(store);
    let genesis = chain.tip().unwrap().hash;

    // 本体は渡さず、ヘッダだけを 5 個受け取る。
    let mut parent = genesis;
    let mut headers = Vec::new();
    for i in 0..5 {
        let block = build_on(&chain, parent, 100 + i);
        parent = block.header.hash();
        assert_eq!(
            chain
                .accept_header(&block.header, &AcceptAnyPow, NOW)
                .unwrap(),
            HeaderOutcome::New
        );
        headers.push(block);
    }

    assert_eq!(
        chain.tip().unwrap().hash,
        genesis,
        "the tip should not move"
    );
    assert_eq!(chain.height().unwrap(), 0);
    assert_eq!(
        chain.best_header().unwrap().height(),
        5,
        "headers run ahead"
    );
    assert_eq!(chain.indexed_blocks().unwrap(), 6);

    for header in &headers {
        let entry = entry_of(&chain, &header.header.hash());
        assert_eq!(entry.status, BlockStatus::HeaderOnly);
        assert!(!entry.has_body());
    }

    // 本体を古い順に渡すと、そのつど先端が伸びる。
    for (i, block) in headers.into_iter().enumerate() {
        let hash = block.header.hash();
        assert_eq!(
            chain.accept_block(block, &AcceptAnyPow, NOW).unwrap(),
            AcceptOutcome::ExtendedTip
        );
        assert_eq!(chain.tip().unwrap().hash, hash);
        assert_eq!(chain.height().unwrap(), i as u64 + 1);
        assert_eq!(entry_of(&chain, &hash).status, BlockStatus::FullyValid);
    }
    assert!(chain.missing_bodies(100).unwrap().is_empty());
}

/// 同じヘッダを二度受け取っても増えないこと。
pub fn a_known_header_is_not_added_twice<S: ChainStore>(store: S) {
    let mut chain = open(store);
    let genesis = chain.tip().unwrap().hash;
    let block = build_on(&chain, genesis, 1);

    assert_eq!(
        chain
            .accept_header(&block.header, &AcceptAnyPow, NOW)
            .unwrap(),
        HeaderOutcome::New
    );
    assert_eq!(
        chain
            .accept_header(&block.header, &AcceptAnyPow, NOW)
            .unwrap(),
        HeaderOutcome::Known
    );
    assert_eq!(chain.indexed_blocks().unwrap(), 2);

    // 本体を受け取った後は、ヘッダは「既知」のままである。
    chain
        .accept_block(block.clone(), &AcceptAnyPow, NOW)
        .unwrap();
    assert_eq!(
        chain
            .accept_header(&block.header, &AcceptAnyPow, NOW)
            .unwrap(),
        HeaderOutcome::Known
    );
    // 本体も二度目は重複として扱う。
    assert_eq!(
        chain.accept_block(block, &AcceptAnyPow, NOW).unwrap(),
        AcceptOutcome::Duplicate
    );
    assert_eq!(chain.indexed_blocks().unwrap(), 2);
}

/// 本体が飛び飛びに届いても、そろうまで先端が動かないこと。
///
/// 取り寄せは複数のピアに割り振るため、順序が入れ替わって届きうる。
/// 途中が欠けたまま先へ進むと、検証していないブロックを飛ばして
/// UTXO を更新することになる。
pub fn bodies_arriving_out_of_order_wait_for_their_parents<S: ChainStore>(store: S) {
    let mut chain = open(store);
    let genesis = chain.tip().unwrap().hash;

    let mut parent = genesis;
    let mut blocks = Vec::new();
    for i in 0..4 {
        let block = build_on(&chain, parent, 200 + i);
        parent = block.header.hash();
        chain
            .accept_header(&block.header, &AcceptAnyPow, NOW)
            .unwrap();
        blocks.push(block);
    }

    // 高さ 3 と 4 を先に渡す。親がまだ無いので繋がらない。
    let fourth = blocks.pop().unwrap();
    let third = blocks.pop().unwrap();
    assert_eq!(
        chain.accept_block(fourth, &AcceptAnyPow, NOW).unwrap(),
        AcceptOutcome::SideChain,
        "do not switch while the parent's body is missing"
    );
    assert_eq!(chain.height().unwrap(), 0);
    assert_eq!(
        chain.accept_block(third, &AcceptAnyPow, NOW).unwrap(),
        AcceptOutcome::SideChain
    );
    assert_eq!(chain.height().unwrap(), 0);

    // 高さ 2 を渡してもまだ足りない。
    let second = blocks.pop().unwrap();
    assert_eq!(
        chain.accept_block(second, &AcceptAnyPow, NOW).unwrap(),
        AcceptOutcome::SideChain
    );
    assert_eq!(chain.height().unwrap(), 0);

    // 最後の欠けが埋まると、一気に 4 つ繋がる。
    let first = blocks.pop().unwrap();
    let outcome = chain.accept_block(first, &AcceptAnyPow, NOW).unwrap();
    assert!(
        matches!(outcome, AcceptOutcome::Reorganized(_)),
        "all four connect at once: {outcome:?}"
    );
    assert_eq!(chain.height().unwrap(), 4);
    assert!(chain.missing_bodies(100).unwrap().is_empty());
}

/// 取り寄せるべき本体が、古い順に挙がること。
pub fn missing_bodies_are_listed_oldest_first<S: ChainStore>(store: S) {
    let mut chain = open(store);
    let genesis = chain.tip().unwrap().hash;

    let mut parent = genesis;
    let mut expected = Vec::new();
    for i in 0..6 {
        let block = build_on(&chain, parent, 300 + i);
        parent = block.header.hash();
        chain
            .accept_header(&block.header, &AcceptAnyPow, NOW)
            .unwrap();
        expected.push(block.header.hash());
    }

    assert_eq!(chain.missing_bodies(100).unwrap(), expected);
    assert_eq!(
        chain.missing_bodies(2).unwrap(),
        expected[..2].to_vec(),
        "the bound takes effect from the older end"
    );
    assert!(chain.missing_bodies(0).unwrap().is_empty());
}

/// 本体を持つブロックの下に穴があっても、その穴が挙がること。
///
/// 本体は順不同に届くので、上のほうだけ届いて途中が欠けることは普通に
/// ある。取り寄せ中の一覧はメモリにしか無いので、その状態で再起動すると
/// 穴を覚えているのはインデックスだけになる。**先端側から遡って本体に
/// 当たったところで探すのをやめると、穴は二度と取り寄せられない。**
pub fn a_gap_below_held_bodies_is_still_listed<S: ChainStore>(store: S) {
    let mut chain = open(store);
    let genesis = chain.tip().unwrap().hash;

    let mut parent = genesis;
    let mut blocks = Vec::new();
    for i in 0..6 {
        let block = build_on(&chain, parent, 400 + i);
        parent = block.header.hash();
        chain
            .accept_header(&block.header, &AcceptAnyPow, NOW)
            .unwrap();
        blocks.push(block);
    }
    let gap = blocks[1].header.hash();

    // 高さ 1 は繋がる。高さ 2 を飛ばして 3〜6 を渡す。
    let mut rest = blocks.split_off(1);
    let second = rest.remove(0);
    chain
        .accept_block(blocks.pop().unwrap(), &AcceptAnyPow, NOW)
        .unwrap();
    for block in rest {
        chain.accept_block(block, &AcceptAnyPow, NOW).unwrap();
    }
    assert_eq!(chain.height().unwrap(), 1);

    assert_eq!(
        chain.missing_bodies(100).unwrap(),
        vec![gap],
        "the gap under the held bodies must be fetched"
    );

    // 穴が埋まれば一気に繋がり、欲しいものは無くなる。
    chain.accept_block(second, &AcceptAnyPow, NOW).unwrap();
    assert_eq!(chain.height().unwrap(), 6);
    assert!(chain.missing_bodies(100).unwrap().is_empty());
}

/// 自分の枝の上にいても、重い枝の穴が挙がること。
///
/// 0.1.0 / 0.1.1 はシードの境界で正しいブロックに無効の印を付け、自分
/// だけの枝を伸ばしたまま止まった。印の後に届いた本体は捨てていたので、
/// 印を外した後の重い枝は**本体が虫食いになっている。** 穴を拾えないと、
/// 軽い自分の枝に留まったまま何も言わずに止まる。
pub fn a_gap_in_a_heavier_branch_is_listed_from_a_side_tip<S: ChainStore>(store: S) {
    let mut chain = open(store);
    let genesis = chain.tip().unwrap().hash;

    // 自分の枝。高さ 2 まで繋いである。
    let ours = extend(&mut chain, genesis, 2, 7);
    assert_eq!(chain.tip().unwrap().hash, ours[1]);

    // 重い枝。ヘッダはすべて知っていて、本体は高さ 2 だけが無い。
    let mut parent = genesis;
    let mut theirs = Vec::new();
    for i in 0..5 {
        let block = build_on(&chain, parent, 500 + i);
        parent = block.header.hash();
        chain
            .accept_header(&block.header, &AcceptAnyPow, NOW)
            .unwrap();
        theirs.push(block);
    }
    let gap = theirs[1].clone();
    for (i, block) in theirs.iter().enumerate() {
        if i != 1 {
            chain
                .accept_block(block.clone(), &AcceptAnyPow, NOW)
                .unwrap();
        }
    }
    assert_eq!(
        chain.tip().unwrap().hash,
        ours[1],
        "stuck on our own branch"
    );

    assert_eq!(
        chain.missing_bodies(100).unwrap(),
        vec![gap.header.hash()],
        "the gap in the heavier branch must be fetched"
    );

    let outcome = chain.accept_block(gap, &AcceptAnyPow, NOW).unwrap();
    assert!(
        matches!(outcome, AcceptOutcome::Reorganized(ref r) if r.disconnected.len() == 2),
        "our two blocks are dropped: {outcome:?}"
    );
    assert_eq!(chain.height().unwrap(), 5);
    assert!(chain.missing_bodies(100).unwrap().is_empty());
}

/// `getheaders` にロケータの分岐点から答えること。
pub fn headers_are_served_from_the_fork_point<S: ChainStore>(store: S) {
    let mut chain = open(store);
    let genesis = chain.tip().unwrap().hash;
    let hashes = extend(&mut chain, genesis, 10, 1);

    // 何も知らない相手 (ジェネシスだけ) には高さ 1 から返す。
    let all = chain.headers_after(&[genesis], &Hash::ZERO, 100).unwrap();
    assert_eq!(all.len(), 10);
    assert_eq!(all[0].height, 1);
    assert_eq!(all[9].height, 10);

    // 高さ 4 まで知っている相手には高さ 5 から返す。
    let after = chain.headers_after(&[hashes[3]], &Hash::ZERO, 100).unwrap();
    assert_eq!(after.len(), 6);
    assert_eq!(after[0].height, 5);

    // 上限が効くこと。
    let capped = chain.headers_after(&[genesis], &Hash::ZERO, 3).unwrap();
    assert_eq!(capped.len(), 3);
    assert_eq!(capped[2].height, 3);

    // stop で打ち切ること (そのヘッダを含む)。
    let stopped = chain.headers_after(&[genesis], &hashes[2], 100).unwrap();
    assert_eq!(stopped.len(), 3);
    assert_eq!(stopped[2].height, 3);

    // 先端まで知っている相手には何も返さない。
    assert!(chain
        .headers_after(&[hashes[9]], &Hash::ZERO, 100)
        .unwrap()
        .is_empty());

    // 知らないハッシュしか無いロケータにはジェネシスの次から返す。
    let unknown = oag_primitives::hash::block_hash(b"unknown block");
    let from_scratch = chain.headers_after(&[unknown], &Hash::ZERO, 100).unwrap();
    assert_eq!(from_scratch.len(), 10);
    assert_eq!(from_scratch[0].height, 1);
}

/// 無効と分かっているブロックのヘッダを受け取っても、蘇らないこと。
pub fn a_header_for_an_invalid_block_is_refused<S: ChainStore>(store: S) {
    let mut chain = open(store);
    let genesis = chain.tip().unwrap().hash;

    // 報酬を取りすぎたブロックを作る。ヘッダは正しいので、接続を試みて
    // 初めて無効と判明する。
    let mut bad = build_on(&chain, genesis, 7);
    bad.transactions[0].outputs[0].amount = Amount::from_oag(1_000).unwrap();
    bad.header.merkle_root = merkle::merkle_root(&[bad.transactions[0].txid()]).unwrap();
    let bad_hash = bad.header.hash();
    let header = bad.header;

    assert_eq!(
        chain.accept_block(bad, &AcceptAnyPow, NOW).unwrap(),
        AcceptOutcome::SideChain,
        "connecting fails, so it does not become the tip"
    );
    assert_eq!(entry_of(&chain, &bad_hash).status, BlockStatus::Invalid);
    assert_eq!(chain.tip().unwrap().hash, genesis);

    // 同じヘッダを送り直されても受け付けない。
    assert!(matches!(
        chain.accept_header(&header, &AcceptAnyPow, NOW),
        Err(ChainError::InvalidAncestor(_))
    ));
    assert_eq!(entry_of(&chain, &bad_hash).status, BlockStatus::Invalid);
}

/// 調整を止めたチェーンでは、いくら速く積んでも難易度が動かないこと。
///
/// regtest はこの設定で動く。止めていないと、ブロックを速く積むほど
/// 難易度が上がり、コインベースの成熟を待つだけで現実的でない時間が
/// かかる。試験用のネットワークとして使い物にならない。
pub fn the_difficulty_never_moves_without_retargeting<S: ChainStore>(store: S) {
    let mut chain =
        Chain::open(store, genesis(), DIFFICULTY, Retarget::Disabled).expect("genesis is valid");
    let tip = chain.tip().unwrap().hash;

    // 窓幅を大きく超えて積む。調整が効いていれば必ず動く長さである。
    let window = oag_pow::lwma::WINDOW;
    let hashes = extend(&mut chain, tip, window + 30, 1);
    assert_eq!(chain.height().unwrap(), window as u64 + 30);

    for hash in &hashes {
        assert_eq!(
            entry_of(&chain, hash).header.difficulty,
            DIFFICULTY,
            "difficulty is moving"
        );
    }
    assert_eq!(
        chain
            .expected_difficulty_for_child_of(hashes.last().unwrap())
            .unwrap(),
        DIFFICULTY
    );
}

/// 調整が有効なら、速く積んだ分だけ難易度が上がること。
///
/// 上の裏返しである。**両方を確かめないと、片方を試したつもりで
/// 両方止まっていても気づけない。**
pub fn the_difficulty_rises_when_blocks_come_too_fast<S: ChainStore>(store: S) {
    let mut chain = open(store);
    let tip = chain.tip().unwrap().hash;

    // build_on は 60 秒間隔で刻む。目標どおりなので難易度は動かない。
    let window = oag_pow::lwma::WINDOW;
    let steady = extend(&mut chain, tip, window + 5, 1);
    assert_eq!(
        chain
            .expected_difficulty_for_child_of(steady.last().unwrap())
            .unwrap(),
        DIFFICULTY,
        "difficulty moves as the target interval intends"
    );
}

// ━━━━━━━━ 相対 locktime (SPEC §7.5) ━━━━━━━━

/// 相対 locktime の強制が、決めた高さのブロックから始まること。
///
/// 同じブロックを、強制の始まる高さだけが違う 2 本のチェーンに渡す。
/// 開始より前の高さなら通り、開始の高さちょうどなら弾かれる。時間で指定
/// したものを使うので、過去の Median Time Past を枝から引く経路も通る。
pub fn relative_locktime_starts_at_its_height<S: ChainStore>(before: S, at: S) {
    let mut before = open(before);
    let mut at = open(at);

    // 高さ 1 のコインベースを、鍵を握っている出力にする。
    let key = SecretKey::generate();
    let coin_lock = Lock::pay_to_pubkey(&key.public_key());
    let genesis_hash = before.tip().unwrap().hash;
    let mut first = build_on(&before, genesis_hash, 1);
    first.transactions[0].outputs[0].lock = coin_lock.clone();
    first.header.merkle_root = merkle::merkle_root(&[first.transactions[0].txid()]).unwrap();
    let coin = OutPoint::new(first.transactions[0].txid(), 0);
    let coin_output = first.transactions[0].outputs[0].clone();

    let mut blocks = vec![first];
    // 成熟するまで積む。使うブロックは高さ 1 + COINBASE_MATURITY。
    let spend_height = 1 + oag_consensus::params::COINBASE_MATURITY;
    for chain in [&mut before, &mut at] {
        chain
            .accept_block(blocks[0].clone(), &AcceptAnyPow, NOW)
            .unwrap();
    }
    while before.height().unwrap() + 1 < spend_height {
        let tip = before.tip().unwrap().hash;
        let block = build_on(&before, tip, before.height().unwrap() + 10);
        for chain in [&mut before, &mut at] {
            chain
                .accept_block(block.clone(), &AcceptAnyPow, NOW)
                .unwrap();
        }
        blocks.push(block);
    }

    // 最大の時間 (65,535 × 512 秒、約 388 日) を待つ版数 2 の支払い。
    // ブロックは 60 秒間隔なので、まったく足りない。
    let mut spend = Transaction {
        version: 2,
        inputs: vec![TxInput::new(coin)],
        outputs: vec![TxOutput::new(
            coin_output
                .amount
                .checked_sub("0.01".parse().unwrap())
                .unwrap(),
            coin_lock,
        )],
        locktime: 0,
    };
    spend.inputs[0].sequence = SEQUENCE_TYPE_FLAG | 0xFFFF;
    let msg = sighash(
        &spend,
        std::slice::from_ref(&coin_output),
        0,
        SighashType::DEFAULT,
    )
    .unwrap();
    spend.inputs[0].signature = key.sign(&msg).to_bytes().to_vec();

    let tip = before.tip().unwrap().hash;
    let mut block = build_on(&before, tip, 999);
    block.transactions.push(spend);
    let txids: Vec<Hash> = block.transactions.iter().map(|t| t.txid()).collect();
    block.header.merkle_root = merkle::merkle_root(&txids).unwrap();
    assert_eq!(block.header.height, spend_height);
    let hash = block.header.hash();

    // 開始が 1 つ上なら、まだ強制されない。
    before.set_relative_locktime_height(spend_height + 1);
    assert_eq!(
        before
            .accept_block(block.clone(), &AcceptAnyPow, NOW)
            .unwrap(),
        AcceptOutcome::ExtendedTip
    );
    assert_eq!(before.tip().unwrap().hash, hash);

    // 開始がこの高さなら弾かれ、先端は動かない。
    at.set_relative_locktime_height(spend_height);
    let _ = at.accept_block(block, &AcceptAnyPow, NOW);
    assert_eq!(at.height().unwrap(), spend_height - 1);
    assert_eq!(entry_of(&at, &hash).status, BlockStatus::Invalid);
}

// ━━━━━━━━ 同じブロックの中で使われた出力 ━━━━━━━━

/// 自分の中で作った出力を自分の中で使うブロックを、リオーグで外せること。
///
/// mempool は親子を受け付け、ひな形は親を子の前に並べる。おつりをすぐ
/// 使う送金があれば、普通に掘ったブロックがこの形になる。
///
/// 0.4.2 までは、こういうブロックを巻き戻せなかった。巻き戻し情報の
/// 「作った出力」を全部消そうとして、子が使ったぶんで `MissingUtxo` に
/// なった。記憶域の失敗として扱われるので無効の印も付かず、そのノードは
/// それより前に戻るリオーグを永久にできず、古い枝に取り残された。
pub fn a_block_spending_its_own_outputs_can_be_reorged_away<S: ChainStore>(store: S) {
    let mut chain = open(store);

    // 高さ 1 のコインベースを、鍵を握っている出力にする。
    let key = SecretKey::generate();
    let lock = Lock::pay_to_pubkey(&key.public_key());
    let genesis_hash = chain.tip().unwrap().hash;
    let mut first = build_on(&chain, genesis_hash, 1);
    first.transactions[0].outputs[0].lock = lock.clone();
    first.header.merkle_root = merkle::merkle_root(&[first.transactions[0].txid()]).unwrap();
    let coin = OutPoint::new(first.transactions[0].txid(), 0);
    let coin_output = first.transactions[0].outputs[0].clone();
    chain.accept_block(first, &AcceptAnyPow, NOW).unwrap();

    // 成熟するまで積む。
    let spend_height = 1 + oag_consensus::params::COINBASE_MATURITY;
    let tip = chain.tip().unwrap().hash;
    extend(&mut chain, tip, (spend_height - 2) as usize, 2);
    assert_eq!(chain.height().unwrap(), spend_height - 1);

    let sign = |tx: &mut Transaction, spent: &TxOutput| {
        let msg = sighash(tx, std::slice::from_ref(spent), 0, SighashType::DEFAULT).unwrap();
        tx.inputs[0].signature = key.sign(&msg).to_bytes().to_vec();
    };

    // 親: コインを使う。子: 親の出力を、同じブロックの中で使う。
    let mut parent = Transaction {
        version: CURRENT_TX_VERSION,
        inputs: vec![TxInput::new(coin)],
        outputs: vec![TxOutput::new(
            coin_output
                .amount
                .checked_sub("0.01".parse().unwrap())
                .unwrap(),
            lock.clone(),
        )],
        locktime: 0,
    };
    sign(&mut parent, &coin_output);
    let change = OutPoint::new(parent.txid(), 0);
    let change_output = parent.outputs[0].clone();

    let mut child = Transaction {
        version: CURRENT_TX_VERSION,
        inputs: vec![TxInput::new(change)],
        outputs: vec![TxOutput::new(
            change_output
                .amount
                .checked_sub("0.01".parse().unwrap())
                .unwrap(),
            lock,
        )],
        locktime: 0,
    };
    sign(&mut child, &change_output);
    let paid = OutPoint::new(child.txid(), 0);

    let fork_point = chain.tip().unwrap().hash;
    let mut block = build_on(&chain, fork_point, 999);
    block.transactions.push(parent);
    block.transactions.push(child);
    let txids: Vec<Hash> = block.transactions.iter().map(|t| t.txid()).collect();
    block.header.merkle_root = merkle::merkle_root(&txids).unwrap();
    let chained = block.header.hash();
    assert_eq!(
        chain.accept_block(block, &AcceptAnyPow, NOW).unwrap(),
        AcceptOutcome::ExtendedTip
    );

    let utxo_has = |chain: &Chain<S>, outpoint: &OutPoint| {
        chain
            .utxo_view()
            .expect("a view can be taken")
            .get(outpoint)
            .expect("readable")
            .is_some()
    };
    assert!(!utxo_has(&chain, &coin), "the coin was spent");
    assert!(
        !utxo_has(&chain, &change),
        "the change was spent in the same block"
    );
    assert!(utxo_has(&chain, &paid));

    // 分岐点から、より重い枝を積む。
    let mut tip = fork_point;
    let mut side = Vec::new();
    for i in 0..2 {
        let block = build_on(&chain, tip, 5_000 + i);
        tip = block.header.hash();
        side.push(tip);
        let outcome = chain.accept_block(block, &AcceptAnyPow, NOW).unwrap();
        if i == 1 {
            match outcome {
                AcceptOutcome::Reorganized(reorg) => {
                    assert_eq!(reorg.disconnected, vec![chained]);
                    assert_eq!(reorg.connected, side);
                }
                other => panic!("it did not reorg: {other:?}"),
            }
        }
    }

    // 外したブロックが使ったコインは戻り、作ったものはどちらも残らない。
    assert_eq!(chain.tip().unwrap().hash, *side.last().unwrap());
    assert!(utxo_has(&chain, &coin), "the coin came back");
    assert!(!utxo_has(&chain, &change), "the change must not reappear");
    assert!(!utxo_has(&chain, &paid), "the payment was undone");
}

// ━━━━━━━━ 採掘者の印による有効化 (SPEC §10.9) ━━━━━━━━

/// 試験用の小さな有効化の決まり。
///
/// 区切りは 4 ブロック、閾値は 3。数え始めは高さ 124 で、高さ 1 の
/// コインベースが成熟する (1 + 120) より後に置いてある。
pub const TEST_DEPLOYMENT: Deployment = Deployment {
    bit: 1,
    start_height: 124,
    timeout_height: 124 + 4 * 50,
    period: 4,
    threshold: 3,
};

/// 印を付けたブロックの version。
const SIGNAL: u32 = TEST_DEPLOYMENT.version_bits();

/// 印で有効化するチェーンを起こす。
pub fn open_signalled<S: ChainStore>(store: S, deployment: Deployment) -> Chain<S> {
    let mut chain = open(store);
    chain.set_commitment_activation(Activation::Signalled(deployment));
    chain
}

/// 高さ 1 のコインベースを、鍵で使える出力にして積む。
fn fund_at_height_one<S: ChainStore>(chain: &mut Chain<S>) -> (SecretKey, OutPoint, TxOutput) {
    let key = SecretKey::generate();
    let genesis_hash = chain.tip().unwrap().hash;
    let mut first = build_on(chain, genesis_hash, 1);
    first.transactions[0].outputs[0].lock = Lock::pay_to_pubkey(&key.public_key());
    first.header.merkle_root = merkle::merkle_root(&[first.transactions[0].txid()]).unwrap();
    let coin = OutPoint::new(first.transactions[0].txid(), 0);
    let output = first.transactions[0].outputs[0].clone();
    chain.accept_block(first, &AcceptAnyPow, NOW).unwrap();
    (key, coin, output)
}

/// `parent` の上に、`version` を付けたブロックを積む。返すのはそのブロック。
fn push_with_version<S: ChainStore>(
    chain: &mut Chain<S>,
    parent: Hash,
    salt: u64,
    version: u32,
    extra: Vec<Transaction>,
) -> Result<(Block, AcceptOutcome), ChainError> {
    let mut block = build_on(chain, parent, salt);
    block.header.version = version;
    block.transactions.extend(extra);
    let txids: Vec<Hash> = block.transactions.iter().map(|t| t.txid()).collect();
    block.header.merkle_root = merkle::merkle_root(&txids).unwrap();
    let outcome = chain.accept_block(block.clone(), &AcceptAnyPow, NOW)?;
    Ok((block, outcome))
}

/// 先端から、`signal(高さ)` の答えに従って印を付けながら `count` 個積む。
fn extend_signalling<S: ChainStore>(
    chain: &mut Chain<S>,
    count: u64,
    salt: u64,
    signal: impl Fn(u64) -> bool,
) -> Vec<Block> {
    let mut blocks = Vec::new();
    for i in 0..count {
        let tip = chain.tip().unwrap();
        let version = if signal(tip.height() + 1) { SIGNAL } else { 0 };
        let (block, _) =
            push_with_version(chain, tip.hash, salt * 1_000_000 + i, version, vec![]).unwrap();
        blocks.push(block);
    }
    blocks
}

/// `spent` を使い、`outputs` を並べた取引。版数 0 なら鍵で署名する。
fn pay(key: &SecretKey, from: OutPoint, spent: &TxOutput, outputs: Vec<TxOutput>) -> Transaction {
    let mut tx = Transaction {
        version: CURRENT_TX_VERSION,
        inputs: vec![TxInput::new(from)],
        outputs,
        locktime: 0,
    };
    if spent.lock.version() == 0 {
        let msg = sighash(&tx, std::slice::from_ref(spent), 0, SighashType::DEFAULT).unwrap();
        tx.inputs[0].signature = key.sign(&msg).to_bytes().to_vec();
    }
    tx
}

fn note(amount: Amount) -> TxOutput {
    TxOutput::new(amount, Lock::commitment(b"orange".to_vec()).unwrap())
}

fn state_after<S: ChainStore>(chain: &Chain<S>) -> DeploymentState {
    let tip = chain.tip().unwrap().hash;
    chain.deployment_state_for_child_of(&tip).unwrap()
}

fn in_utxo<S: ChainStore>(chain: &Chain<S>, outpoint: &OutPoint) -> bool {
    chain
        .utxo_view()
        .expect("a view can be taken")
        .get(outpoint)
        .expect("readable")
        .is_some()
}

/// 印が閾値に届くと `LockedIn` になり、次の区切りで規則が効くこと。
///
/// - 猶予の区切りで印が消えても取り消されない。
/// - 効く前のブロックでは、金額付きのコミットメント出力も、末尾 `0x00` の
///   署名も通る (さかのぼって無効にしない)。
/// - 効いた後のブロックでは、どちらも無効になる。
/// - 効く前に作られたコミットメント出力は、効いた後も UTXO に残り、使える。
/// - 0.4.3 と同じ形 (同じブロックの中で親の出力を子が使う) を混ぜても崩れない。
pub fn signals_lock_in_and_then_activate_the_rules<S: ChainStore>(store: S) {
    let mut chain = open_signalled(store, TEST_DEPLOYMENT);
    let (key, coin, coin_output) = fund_at_height_one(&mut chain);
    let lock = Lock::pay_to_pubkey(&key.public_key());

    // 高さ 122 まで積む。次 (123) はまだ数え始める前の区切り。
    extend_signalling(&mut chain, 121, 1, |_| false);
    assert_eq!(chain.height().unwrap(), 122);
    assert_eq!(state_after(&chain), DeploymentState::Defined);
    let tip = chain.tip().unwrap().hash;
    assert_eq!(chain.block_version_for_child_of(&tip).unwrap(), 0);
    assert_eq!(chain.rules_for_child_of(&tip).unwrap(), Rules::NONE);

    // 高さ 123 を積むと、次 (124) から数え始める。印を付けるよう求められる。
    extend_signalling(&mut chain, 1, 2, |_| false);
    assert_eq!(state_after(&chain), DeploymentState::Started);
    let tip = chain.tip().unwrap().hash;
    assert_eq!(chain.block_version_for_child_of(&tip).unwrap(), SIGNAL);
    assert_eq!(chain.signals_so_far(&tip).unwrap(), Some((0, 4)));

    // 124〜127 のうち 3 個に印 (126 だけ付けない)。閾値ちょうど。
    extend_signalling(&mut chain, 4, 3, |h| h != 126);
    assert_eq!(chain.height().unwrap(), 127);
    let tip = chain.tip().unwrap().hash;
    assert_eq!(chain.signals_so_far(&tip).unwrap(), Some((3, 4)));
    assert_eq!(state_after(&chain), DeploymentState::LockedIn);
    assert_eq!(
        chain.block_version_for_child_of(&tip).unwrap(),
        SIGNAL,
        "keep signalling through the grace period"
    );

    // 猶予の区切り (128〜131) では誰も印を付けない。それでも取り消されない。
    extend_signalling(&mut chain, 3, 4, |_| false);
    assert_eq!(chain.height().unwrap(), 130);
    assert_eq!(state_after(&chain), DeploymentState::LockedIn);
    let tip = chain.tip().unwrap().hash;
    assert_eq!(chain.rules_for_child_of(&tip).unwrap(), Rules::NONE);

    // 高さ 131 は効く前の最後のブロック。金額付きのコミットメント出力と、
    // 末尾 0x00 の署名と、同じブロックの中で親の出力を子が使う形を混ぜる。
    let one = Amount::from_atomic(1).unwrap();
    let change = coin_output.amount.checked_sub(Amount::ONE_OAG).unwrap();
    let mut parent = pay(
        &key,
        coin,
        &coin_output,
        vec![TxOutput::new(change, lock.clone()), note(one)],
    );
    parent.inputs[0].signature.push(0x00);
    let parent_pay = OutPoint::new(parent.txid(), 0);
    let old_note = OutPoint::new(parent.txid(), 1);
    let child = pay(
        &key,
        parent_pay,
        &parent.outputs[0],
        vec![TxOutput::new(
            change.checked_sub(Amount::ONE_OAG).unwrap(),
            lock.clone(),
        )],
    );
    let child_out = OutPoint::new(child.txid(), 0);
    let child_output = child.outputs[0].clone();
    let (_, outcome) =
        push_with_version(&mut chain, tip, 131, SIGNAL, vec![parent, child]).unwrap();
    assert_eq!(outcome, AcceptOutcome::ExtendedTip);
    assert_eq!(chain.height().unwrap(), 131);
    assert!(!in_utxo(&chain, &coin));
    assert!(!in_utxo(&chain, &parent_pay), "spent in the same block");
    assert!(in_utxo(&chain, &child_out));
    assert!(
        in_utxo(&chain, &old_note),
        "before activation a commitment is just an unknown version"
    );

    // ここから規則が効く。印はもう付けない。
    assert_eq!(state_after(&chain), DeploymentState::Active);
    let tip = chain.tip().unwrap().hash;
    assert_eq!(chain.rules_for_child_of(&tip).unwrap(), Rules::ALL);
    assert_eq!(chain.block_version_for_child_of(&tip).unwrap(), 0);

    // 末尾 0x00 の署名は無効。
    let mut padded = pay(
        &key,
        child_out,
        &child_output,
        vec![TxOutput::new(Amount::ONE_OAG, lock.clone())],
    );
    padded.inputs[0].signature.push(0x00);
    let bad = push_with_version(&mut chain, tip, 132, 0, vec![padded]);
    assert!(bad.is_ok(), "an invalid block is recorded, not an error");
    assert_eq!(chain.tip().unwrap().hash, tip, "the tip did not move");

    // 金額付きのコミットメント出力も無効。
    let costly = pay(
        &key,
        child_out,
        &child_output,
        vec![TxOutput::new(Amount::ONE_OAG, lock.clone()), note(one)],
    );
    let _ = push_with_version(&mut chain, tip, 133, 0, vec![costly]);
    assert_eq!(chain.tip().unwrap().hash, tip, "the tip did not move");

    // 金額 0 のコミットメント出力は通るが、UTXO には入らない。同じ
    // ブロックの中で使おうとする取引は、無いものを使うので無効になる。
    let maker = pay(
        &key,
        child_out,
        &child_output,
        vec![
            TxOutput::new(Amount::ONE_OAG, lock.clone()),
            note(Amount::ZERO),
        ],
    );
    let new_note = OutPoint::new(maker.txid(), 1);
    let taker = pay(
        &key,
        new_note,
        &maker.outputs[1],
        vec![TxOutput::new(Amount::ZERO, lock.clone())],
    );
    let _ = push_with_version(&mut chain, tip, 134, 0, vec![maker.clone(), taker]);
    assert_eq!(chain.tip().unwrap().hash, tip, "the tip did not move");

    let made = pay(
        &key,
        child_out,
        &child_output,
        vec![
            TxOutput::new(Amount::ONE_OAG, lock.clone()),
            note(Amount::ZERO),
        ],
    );
    let made_note = OutPoint::new(made.txid(), 1);
    let made_pay = OutPoint::new(made.txid(), 0);
    let (_, outcome) = push_with_version(&mut chain, tip, 135, 0, vec![made]).unwrap();
    assert_eq!(outcome, AcceptOutcome::ExtendedTip);
    assert!(!in_utxo(&chain, &made_note), "a commitment is not a coin");
    assert!(in_utxo(&chain, &made_pay));

    // 効く前に作られたコミットメント出力は、そのまま誰でも使える。
    let tip = chain.tip().unwrap().hash;
    let sweep = Transaction {
        version: CURRENT_TX_VERSION,
        inputs: vec![TxInput::new(old_note)],
        outputs: vec![TxOutput::new(one, lock)],
        locktime: 0,
    };
    let (_, outcome) = push_with_version(&mut chain, tip, 136, 0, vec![sweep]).unwrap();
    assert_eq!(outcome, AcceptOutcome::ExtendedTip);
    assert!(!in_utxo(&chain, &old_note));
}

/// 閾値に届かなければ有効にならず、期限で `Failed` になって二度と戻らないこと。
pub fn too_few_signals_time_out<S: ChainStore>(store: S) {
    let deployment = Deployment {
        timeout_height: 132,
        ..TEST_DEPLOYMENT
    };
    let mut chain = open_signalled(store, deployment);
    extend_signalling(&mut chain, 123, 1, |_| false);
    assert_eq!(state_after(&chain), DeploymentState::Started);

    // 124〜127 は 2 個だけ。閾値に 1 足りない。
    extend_signalling(&mut chain, 4, 2, |h| h % 2 == 0);
    assert_eq!(state_after(&chain), DeploymentState::Started);

    // 128〜131 も 2 個。次の区切り (132) は期限なので失敗。
    extend_signalling(&mut chain, 4, 3, |h| h % 2 == 0);
    assert_eq!(state_after(&chain), DeploymentState::Failed);

    // その後に全員が印を付けても戻らない。印を付けるよう求めることもない。
    extend_signalling(&mut chain, 8, 4, |_| true);
    assert_eq!(state_after(&chain), DeploymentState::Failed);
    let tip = chain.tip().unwrap().hash;
    assert_eq!(chain.block_version_for_child_of(&tip).unwrap(), 0);
    assert_eq!(chain.rules_for_child_of(&tip).unwrap(), Rules::NONE);
}

/// 有効化の境目をまたぐリオーグで、それぞれの枝の規則に従うこと。
///
/// 本線は印を付けて規則が効き、効いた後のブロックで 0.4.3 と同じ形の
/// 取引とコミットメント出力を持つ。脇の枝は印を付けず、規則が効かない
/// まま、同じコインで金額付きのコミットメント出力を作る。
///
/// 脇の枝が重くなって切り替わり、また本線が重くなって戻る。最後の UTXO が、
/// 本線だけを直接積んだものと一致することを確かめる。
pub fn a_reorg_across_the_activation_follows_each_branch<S: ChainStore>(
    forked_store: S,
    direct_store: S,
) {
    let mut forked = open_signalled(forked_store, TEST_DEPLOYMENT);
    let (key, coin, coin_output) = fund_at_height_one(&mut forked);
    let lock = Lock::pay_to_pubkey(&key.public_key());
    extend_signalling(&mut forked, 122, 1, |_| false);
    assert_eq!(forked.height().unwrap(), 123);
    let fork_point = forked.tip().unwrap().hash;

    // ── 本線: 124〜131 は全部印。132 から規則が効く ──
    extend_signalling(&mut forked, 8, 2, |_| true);
    assert_eq!(state_after(&forked), DeploymentState::Active);

    let change = coin_output.amount.checked_sub(Amount::ONE_OAG).unwrap();
    let main_parent = pay(
        &key,
        coin,
        &coin_output,
        vec![TxOutput::new(change, lock.clone()), note(Amount::ZERO)],
    );
    let main_pay = OutPoint::new(main_parent.txid(), 0);
    let main_note = OutPoint::new(main_parent.txid(), 1);
    let main_child = pay(
        &key,
        main_pay,
        &main_parent.outputs[0],
        vec![TxOutput::new(
            change.checked_sub(Amount::ONE_OAG).unwrap(),
            lock.clone(),
        )],
    );
    let main_child_out = OutPoint::new(main_child.txid(), 0);
    let tip = forked.tip().unwrap().hash;
    push_with_version(&mut forked, tip, 132, 0, vec![main_parent, main_child]).unwrap();
    extend_signalling(&mut forked, 1, 3, |_| false);
    assert_eq!(forked.height().unwrap(), 133);
    let main_tip = forked.tip().unwrap().hash;
    assert!(!in_utxo(&forked, &coin));
    assert!(!in_utxo(&forked, &main_note));
    assert!(in_utxo(&forked, &main_child_out));

    // ── 脇の枝: 印を付けない。11 個積んで本線 (10 個) を上回る ──
    let one = Amount::from_atomic(1).unwrap();
    let side_parent = pay(
        &key,
        coin,
        &coin_output,
        vec![TxOutput::new(change, lock.clone()), note(one)],
    );
    let side_pay = OutPoint::new(side_parent.txid(), 0);
    let side_note = OutPoint::new(side_parent.txid(), 1);
    let side_child = pay(
        &key,
        side_pay,
        &side_parent.outputs[0],
        vec![TxOutput::new(
            change.checked_sub(Amount::ONE_OAG).unwrap(),
            lock.clone(),
        )],
    );
    let side_child_out = OutPoint::new(side_child.txid(), 0);
    let mut parent = fork_point;
    let mut side_txs = Some(vec![side_parent, side_child]);
    let mut last = None;
    for i in 0..11u64 {
        let height = 124 + i;
        let extra = if height == 132 {
            side_txs.take().unwrap()
        } else {
            vec![]
        };
        let (block, outcome) = push_with_version(&mut forked, parent, 9_000 + i, 0, extra).unwrap();
        parent = block.header.hash();
        last = Some(outcome);
    }
    match last.unwrap() {
        AcceptOutcome::Reorganized(reorg) => assert_eq!(reorg.depth(), 10),
        other => panic!("it did not reorg: {other:?}"),
    }
    assert_eq!(forked.height().unwrap(), 134);
    assert_eq!(state_after(&forked), DeploymentState::Started);
    let tip = forked.tip().unwrap().hash;
    assert_eq!(forked.rules_for_child_of(&tip).unwrap(), Rules::NONE);
    assert!(!in_utxo(&forked, &coin));
    assert!(
        !in_utxo(&forked, &main_child_out),
        "the main branch was undone"
    );
    assert!(!in_utxo(&forked, &side_pay), "spent in the same block");
    assert!(in_utxo(&forked, &side_child_out));
    assert!(
        in_utxo(&forked, &side_note),
        "without activation a commitment is just an unknown version"
    );

    // ── 本線を 2 個伸ばして (12 個) 戻す ──
    let (block, _) = push_with_version(&mut forked, main_tip, 777, 0, vec![]).unwrap();
    let (_, outcome) = push_with_version(&mut forked, block.header.hash(), 778, 0, vec![]).unwrap();
    match outcome {
        AcceptOutcome::Reorganized(reorg) => assert_eq!(reorg.depth(), 11),
        other => panic!("it did not reorg back: {other:?}"),
    }
    assert_eq!(forked.height().unwrap(), 135);
    assert_eq!(state_after(&forked), DeploymentState::Active);
    assert!(!in_utxo(&forked, &side_note), "the side branch was undone");
    assert!(!in_utxo(&forked, &side_child_out));
    assert!(!in_utxo(&forked, &main_note));
    assert!(in_utxo(&forked, &main_child_out));

    // ── 本線だけを直接積んだものと比べる ──
    let mut direct = open_signalled(direct_store, TEST_DEPLOYMENT);
    let mut path = Vec::new();
    let mut cursor = forked.tip().unwrap().hash;
    while let Some(entry) = forked.entry(&cursor).unwrap() {
        if entry.height() == 0 {
            break;
        }
        path.push(cursor);
        cursor = entry.prev_hash();
    }
    path.reverse();
    for hash in &path {
        let block = forked.store().block(hash).unwrap().expect("held");
        direct.accept_block(block, &AcceptAnyPow, NOW).unwrap();
    }
    assert_eq!(direct.tip().unwrap().hash, forked.tip().unwrap().hash);
    assert_eq!(
        direct.store().utxo_count().unwrap(),
        forked.store().utxo_count().unwrap(),
        "the UTXO after two reorgs differs from a direct build"
    );
    for outpoint in [
        coin,
        main_pay,
        main_note,
        main_child_out,
        side_pay,
        side_note,
        side_child_out,
    ] {
        assert_eq!(
            in_utxo(&direct, &outpoint),
            in_utxo(&forked, &outpoint),
            "{outpoint:?} disagrees"
        );
    }
}

/// 有効化の状態は、ヘッダだけから決まること。
///
/// headers-first の同期では、本体が届く前にヘッダが先まで伸びる。その
/// 時点でも、本体を全部持っているチェーンと同じ答えを返さなければならない。
pub fn the_activation_state_needs_only_headers<S: ChainStore>(full_store: S, header_store: S) {
    let mut full = open_signalled(full_store, TEST_DEPLOYMENT);
    let blocks = extend_signalling(&mut full, 140, 1, |h| (124..132).contains(&h));
    let mut headers = open_signalled(header_store, TEST_DEPLOYMENT);
    for block in &blocks {
        assert_eq!(
            headers
                .accept_header(&block.header, &AcceptAnyPow, NOW)
                .unwrap(),
            HeaderOutcome::New
        );
        let hash = block.header.hash();
        assert_eq!(
            headers.deployment_state_for_child_of(&hash).unwrap(),
            full.deployment_state_for_child_of(&hash).unwrap(),
            "at height {}",
            block.header.height
        );
    }
    assert_eq!(headers.height().unwrap(), 0, "no body was connected");
    let last = blocks.last().unwrap().header.hash();
    assert_eq!(
        headers.deployment_state_for_child_of(&last).unwrap(),
        DeploymentState::Active
    );
}

// ━━━━━━━━ 軽い枝を記憶域に書かない (SPEC §10.5) ━━━━━━━━

/// 最良のヘッダより [`ANTI_DOS_BLOCKS`](crate::chain::ANTI_DOS_BLOCKS) ぶん
/// 以上軽い枝のヘッダは、書かずに手元に置かれること。続きが重くなったら、
/// そのとき祖先ごと書かれること。軽い枝の本体は書かれないこと。
pub fn light_branch_headers_wait_in_memory_until_heavy<S: ChainStore>(store: S) {
    let mut chain = open(store);
    let genesis_hash = chain.tip().unwrap().hash;
    let main = extend(&mut chain, genesis_hash, 200, 1);
    assert_eq!(chain.height().unwrap(), 200);
    let indexed = chain.indexed_blocks().unwrap();

    // 難易度は 1 のまま。累積作業量は高さ + 1、閾値は 201 − 144 = 57。
    // 高さ 10 から分岐させると、高さ 55 までは閾値に届かない。
    let mut parent = main[9];
    let mut fork = Vec::new();
    for height in 11..=55u64 {
        let block = build_on(&chain, parent, 50_000 + height);
        parent = block.header.hash();
        assert_eq!(
            chain
                .accept_header(&block.header, &AcceptAnyPow, NOW)
                .unwrap(),
            HeaderOutcome::Pending,
            "at height {height}"
        );
        fork.push(block);
    }
    assert_eq!(chain.pending_headers(), 45);
    assert_eq!(chain.indexed_blocks().unwrap(), indexed, "nothing written");
    // 知ってはいる。
    assert!(chain.contains(&fork[0].header.hash()).unwrap());
    // 送り直されても重ねて置かない。
    assert_eq!(
        chain
            .accept_header(&fork[3].header, &AcceptAnyPow, NOW)
            .unwrap(),
        HeaderOutcome::Known
    );

    // 軽い枝の本体は書かない。
    let body = fork[20].clone();
    let body_hash = body.header.hash();
    assert_eq!(
        chain.accept_block(body, &AcceptAnyPow, NOW).unwrap(),
        AcceptOutcome::SideChain
    );
    assert!(chain.store().block(&body_hash).unwrap().is_none());
    assert_eq!(chain.indexed_blocks().unwrap(), indexed);

    // 高さ 56 で閾値に届く。祖先 45 個もまとめて書かれる。
    let block = build_on(&chain, parent, 50_056);
    assert_eq!(
        chain
            .accept_header(&block.header, &AcceptAnyPow, NOW)
            .unwrap(),
        HeaderOutcome::New
    );
    assert_eq!(chain.pending_headers(), 0);
    assert_eq!(chain.indexed_blocks().unwrap(), indexed + 46);
    assert_eq!(chain.tip().unwrap().hash, *main.last().unwrap(), "no reorg");
}

/// どれほど深く分岐した枝でも、重くなれば切り替わること。
///
/// リオーグの深さに上限は無い (SPEC §10.6)。軽いうちは本体を捨てるが、
/// 重くなった時点でヘッダが書かれ、捨てた本体は取り寄せ直せる。
pub fn a_deep_but_heavier_branch_still_wins<S: ChainStore>(store: S) {
    let mut chain = open(store);
    let genesis_hash = chain.tip().unwrap().hash;
    let main = extend(&mut chain, genesis_hash, 200, 1);

    // 高さ 10 から分岐し、本線より 1 つ長い枝を本体ごと届ける。
    let mut parent = main[9];
    let mut fork = Vec::new();
    for height in 11..=201u64 {
        let block = build_on(&chain, parent, 70_000 + height);
        parent = block.header.hash();
        fork.push(block.clone());
        chain.accept_block(block, &AcceptAnyPow, NOW).unwrap();
    }
    // 軽いうちに届いた本体は捨てたので、まだ切り替われない。
    assert_eq!(chain.tip().unwrap().hash, *main.last().unwrap());
    let missing = chain.missing_bodies(1_000).unwrap();
    assert!(
        !missing.is_empty(),
        "the dropped bodies are asked for again"
    );

    // 取り寄せ直した本体を届けると、切り替わる。
    for hash in missing {
        let block = fork
            .iter()
            .find(|b| b.header.hash() == hash)
            .unwrap()
            .clone();
        chain.accept_block(block, &AcceptAnyPow, NOW).unwrap();
    }
    assert_eq!(chain.tip().unwrap().hash, parent);
    assert_eq!(chain.height().unwrap(), 201);
}
