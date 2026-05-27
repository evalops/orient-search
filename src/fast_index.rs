//! Persistent local search index for agent-oriented code retrieval.

use crate::repo_index::{
    SearchFilters, SearchResult, best_snippet, finalize_results, is_ignored, language_for,
    matches_filters, round4, token_counts, tokenize,
};
use anyhow::{Context, Result};
use ignore::WalkBuilder;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

const INDEX_VERSION: u32 = 1;
const MAX_FILE_BYTES: u64 = 512_000;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FastIndex {
    pub version: u32,
    pub root: PathBuf,
    pub files: Vec<IndexedPath>,
    pub postings: HashMap<String, Vec<Posting>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexedPath {
    pub path: String,
    pub language: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Posting {
    pub file_id: u32,
    pub count: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexStats {
    pub version: u32,
    pub root: PathBuf,
    pub files: usize,
    pub terms: usize,
}

impl FastIndex {
    pub fn build(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().canonicalize()?;
        let mut files = Vec::new();
        let mut term_files: HashMap<String, Vec<Posting>> = HashMap::new();

        for entry in WalkBuilder::new(&root)
            .hidden(false)
            .filter_entry(|entry| !is_ignored(entry.path()))
            .build()
        {
            let entry = entry?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let metadata = entry.metadata()?;
            if metadata.len() > MAX_FILE_BYTES {
                continue;
            }
            let Some(language) = language_for(path) else {
                continue;
            };
            let text = fs::read_to_string(path).unwrap_or_default();
            if text.contains('\0') {
                continue;
            }
            let rel = path
                .strip_prefix(&root)?
                .to_string_lossy()
                .replace('\\', "/");
            let file_id = files.len() as u32;
            let counts = token_counts(&format!("{rel}\n{text}"));
            files.push(IndexedPath {
                path: rel,
                language,
            });
            for (term, count) in counts {
                term_files.entry(term).or_default().push(Posting {
                    file_id,
                    count: count.min(u16::MAX as usize) as u16,
                });
            }
        }

        Ok(Self {
            version: INDEX_VERSION,
            root,
            files,
            postings: term_files,
        })
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let bytes = fs::read(path.as_ref())
            .with_context(|| format!("read index {}", path.as_ref().display()))?;
        let index = serde_json::from_slice::<Self>(&bytes)
            .with_context(|| format!("parse index {}", path.as_ref().display()))?;
        anyhow::ensure!(
            index.version == INDEX_VERSION,
            "unsupported index version {}",
            index.version
        );
        Ok(index)
    }

    pub fn save(&self, path: impl AsRef<Path>) -> Result<()> {
        if let Some(parent) = path.as_ref().parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path.as_ref(), serde_json::to_vec(self)?)
            .with_context(|| format!("write index {}", path.as_ref().display()))
    }

    pub fn stats(&self) -> IndexStats {
        IndexStats {
            version: self.version,
            root: self.root.clone(),
            files: self.files.len(),
            terms: self.postings.len(),
        }
    }

    pub fn search(&self, query: &str, limit: usize) -> Result<Vec<SearchResult>> {
        self.search_filtered(query, limit, &SearchFilters::default())
    }

    pub fn search_filtered(
        &self,
        query: &str,
        limit: usize,
        filters: &SearchFilters,
    ) -> Result<Vec<SearchResult>> {
        let query_tokens = tokenize(query);
        if query_tokens.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }

        let mut token_postings = query_tokens
            .iter()
            .filter_map(|token| self.postings.get(token).map(|postings| (token, postings)))
            .collect::<Vec<_>>();
        if token_postings.is_empty() {
            return Ok(Vec::new());
        }
        token_postings.sort_by_key(|(_, postings)| postings.len());

        let mut candidate_ids = token_postings[0]
            .1
            .iter()
            .map(|posting| posting.file_id)
            .collect::<HashSet<_>>();
        for (_, postings) in token_postings.iter().skip(1) {
            let ids = postings
                .iter()
                .map(|posting| posting.file_id)
                .collect::<HashSet<_>>();
            candidate_ids.retain(|id| ids.contains(id));
            if candidate_ids.is_empty() {
                break;
            }
        }
        if candidate_ids.is_empty() {
            candidate_ids = token_postings[0]
                .1
                .iter()
                .map(|posting| posting.file_id)
                .collect();
        }

        let posting_maps = token_postings
            .iter()
            .map(|(token, postings)| {
                (
                    (*token).clone(),
                    postings
                        .iter()
                        .map(|posting| (posting.file_id, posting.count))
                        .collect::<HashMap<_, _>>(),
                )
            })
            .collect::<Vec<_>>();
        let results = candidate_ids
            .into_iter()
            .filter(|file_id| {
                self.files
                    .get(*file_id as usize)
                    .is_some_and(|file| matches_filters(&file.path, filters))
            })
            .filter_map(|file_id| self.score_file(file_id, &query_tokens, &posting_maps))
            .collect::<Vec<_>>();

        Ok(finalize_results(results, limit))
    }

    fn score_file(
        &self,
        file_id: u32,
        query_tokens: &[String],
        posting_maps: &[(String, HashMap<u32, u16>)],
    ) -> Option<SearchResult> {
        let file = self.files.get(file_id as usize)?;
        let path_lower = file.path.to_lowercase();
        let mut score = 0.0;
        let mut reasons = Vec::new();

        for (token, postings) in posting_maps {
            let count = postings.get(&file_id).copied().unwrap_or_default();
            let mut token_score = 0.0;
            if count > 0 {
                token_score += 1.0 + (count as f64).ln();
            }
            if path_lower.contains(token) {
                token_score += 8.0;
            }
            if token_score > 0.0 {
                score += token_score;
                reasons.push(token.clone());
            }
        }
        if score == 0.0 {
            return None;
        }

        let text = fs::read_to_string(self.root.join(&file.path)).unwrap_or_default();
        let snippet = if text.is_empty() {
            String::new()
        } else {
            best_snippet(&text, query_tokens)
        };

        Some(SearchResult {
            path: file.path.clone(),
            score: round4(score),
            reason: format!("indexed match {}", reasons.join(", ")),
            snippet,
        })
    }
}
