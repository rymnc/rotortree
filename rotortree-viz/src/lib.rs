use rotortree::{Blake3Hasher, Hash, LeanIMT, TreeSnapshot};
use serde::Serialize;
use wasm_bindgen::prelude::*;

#[derive(Serialize)]
struct InsertResult {
    root: String,
    /// (level, index) pairs for each node touched during insert propagation
    touched: Vec<(usize, usize)>,
}

#[derive(Serialize)]
struct TreeExport {
    size: u64,
    depth: usize,
    root: Option<String>,
    levels: Vec<LevelExport>,
}

#[derive(Serialize)]
struct LevelExport {
    nodes: Vec<String>,
}

#[derive(Serialize)]
struct ProofExport {
    root: String,
    leaf: String,
    leaf_index: u64,
    levels: Vec<ProofLevelExport>,
}

#[derive(Serialize)]
struct ProofLevelExport {
    position: u8,
    sibling_count: u8,
    siblings: Vec<String>,
}

fn hex(h: &Hash) -> String {
    h.iter().map(|b| format!("{b:02x}")).collect()
}

fn hex_short(h: &Hash) -> String {
    format!("{:02x}{:02x}{:02x}{:02x}", h[0], h[1], h[2], h[3])
}

fn hash_from_js(data: &[u8]) -> Hash {
    let hasher = Blake3Hasher;
    rotortree::Hasher::hash_children(&hasher, &[*blake3::hash(data).as_bytes()])
}

/// Compute the touched (level, index) path for an insert at `old_size`.
fn compute_touched<const N: usize>(old_size: u64, new_depth: usize) -> Vec<(usize, usize)> {
    let mut touched = Vec::with_capacity(new_depth + 1);
    let mut idx = old_size as usize;
    for level in 0..=new_depth {
        touched.push((level, idx));
        idx /= N;
    }
    touched
}

fn export_snapshot<const N: usize, const MAX_DEPTH: usize>(
    snap: &TreeSnapshot<N, MAX_DEPTH>,
) -> TreeExport {
    let depth = snap.depth();
    let mut levels = Vec::with_capacity(depth + 1);
    for level in 0..=depth {
        let len = snap.level_len(level);
        let mut nodes = Vec::with_capacity(len);
        for i in 0..len {
            if let Ok(h) = snap.get_node(level, i) {
                nodes.push(hex_short(&h));
            }
        }
        levels.push(LevelExport { nodes });
    }
    TreeExport {
        size: snap.size(),
        depth,
        root: snap.root().map(|h| hex(&h)),
        levels,
    }
}

fn export_proof<const N: usize, const MAX_DEPTH: usize>(
    snap: &TreeSnapshot<N, MAX_DEPTH>,
    leaf_index: u64,
) -> Result<ProofExport, JsError> {
    let proof = snap
        .generate_proof(leaf_index)
        .map_err(|e| JsError::new(&e.to_string()))?;
    let mut levels = Vec::with_capacity(proof.level_count);
    for i in 0..proof.level_count {
        let pl = &proof.levels[i];
        let sc = pl.sibling_count as usize;
        let siblings: Vec<String> = pl.siblings[..sc].iter().map(|h| hex_short(h)).collect();
        levels.push(ProofLevelExport {
            position: pl.position,
            sibling_count: pl.sibling_count,
            siblings,
        });
    }
    Ok(ProofExport {
        root: hex(&proof.root),
        leaf: hex(&proof.leaf),
        leaf_index: proof.leaf_index,
        levels,
    })
}

macro_rules! viz_tree {
    ($name:ident, $n:literal) => {
        #[wasm_bindgen]
        pub struct $name {
            inner: LeanIMT<Blake3Hasher, $n, 20>,
        }

        #[wasm_bindgen]
        impl $name {
            #[wasm_bindgen(constructor)]
            pub fn new() -> Self {
                Self {
                    inner: LeanIMT::new(Blake3Hasher),
                }
            }

            pub fn insert(&mut self, leaf: &[u8]) -> Result<JsValue, JsError> {
                let old_size = self.inner.size();
                let hash = hash_from_js(leaf);
                let root = self
                    .inner
                    .insert(hash)
                    .map_err(|e| JsError::new(&e.to_string()))?;
                let new_depth = self.inner.depth();
                let touched = compute_touched::<$n>(old_size, new_depth);
                let result = InsertResult {
                    root: hex(&root),
                    touched,
                };
                serde_wasm_bindgen::to_value(&result).map_err(|e| JsError::new(&e.to_string()))
            }

            pub fn insert_many(&mut self, count: u32, start: u32) -> Result<JsValue, JsError> {
                let leaves: Vec<Hash> = (start..start + count)
                    .map(|i| hash_from_js(format!("leaf-{i}").as_bytes()))
                    .collect();
                let old_size = self.inner.size();
                let root = self
                    .inner
                    .insert_many(&leaves)
                    .map_err(|e| JsError::new(&e.to_string()))?;
                let new_depth = self.inner.depth();
                let touched = compute_touched::<$n>(old_size, new_depth);
                let result = InsertResult {
                    root: hex(&root),
                    touched,
                };
                serde_wasm_bindgen::to_value(&result).map_err(|e| JsError::new(&e.to_string()))
            }

            pub fn size(&self) -> u64 {
                self.inner.size()
            }

            pub fn depth(&self) -> usize {
                self.inner.depth()
            }

            pub fn root(&self) -> Option<String> {
                self.inner.root().map(|h| hex(&h))
            }

            /// Export the full tree structure as JSON for rendering.
            pub fn export_tree(&self) -> Result<JsValue, JsError> {
                let snap = self.inner.snapshot();
                let export = export_snapshot::<$n, 20>(&snap);
                serde_wasm_bindgen::to_value(&export)
                    .map_err(|e| JsError::new(&e.to_string()))
            }

            /// Generate a Merkle proof for the given leaf index.
            pub fn generate_proof(&self, index: u64) -> Result<JsValue, JsError> {
                let snap = self.inner.snapshot();
                let proof = export_proof::<$n, 20>(&snap, index)?;
                serde_wasm_bindgen::to_value(&proof)
                    .map_err(|e| JsError::new(&e.to_string()))
            }
        }
    };
}

viz_tree!(TreeN2, 2);
viz_tree!(TreeN4, 4);
viz_tree!(TreeN8, 8);
viz_tree!(TreeN16, 16);
