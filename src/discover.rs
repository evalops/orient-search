//! Bounded local repository discovery for shard setup.

use crate::repo_index::{is_ignored, is_manifest_file};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoverOptions {
    pub max_depth: usize,
    pub limit: usize,
}

impl Default for DiscoverOptions {
    fn default() -> Self {
        Self {
            max_depth: 4,
            limit: 500,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoverReport {
    pub root: PathBuf,
    pub max_depth: usize,
    pub limit: usize,
    pub dirs_scanned: usize,
    pub repos_found: usize,
    pub repos: Vec<DiscoveredRepo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveredRepo {
    pub name: String,
    pub path: PathBuf,
    pub depth: usize,
    pub git: bool,
    pub manifests: Vec<String>,
}

pub fn discover_repos(root: impl AsRef<Path>, options: &DiscoverOptions) -> Result<DiscoverReport> {
    let root = root
        .as_ref()
        .canonicalize()
        .with_context(|| format!("discover root {}", root.as_ref().display()))?;
    let limit = options.limit;
    let max_depth = options.max_depth;
    let mut queue = VecDeque::from([(root.clone(), 0usize)]);
    let mut seen_dirs = HashSet::new();
    let mut seen_repos = HashSet::new();
    let mut dirs_scanned = 0usize;
    let mut repos = Vec::new();

    while let Some((dir, depth)) = queue.pop_front() {
        let key = canonical_or_self(&dir);
        if !seen_dirs.insert(key) {
            continue;
        }
        if should_skip_dir(&dir, &root) {
            continue;
        }
        dirs_scanned += 1;

        let candidate = inspect_candidate_repo(&dir, &root, depth)?;
        if let Some(repo) = candidate {
            let repo_key = canonical_or_self(&repo.path);
            if seen_repos.insert(repo_key) {
                repos.push(repo);
                if limit > 0 && repos.len() >= limit {
                    break;
                }
            }
        }

        if depth >= max_depth {
            continue;
        }

        for child in sorted_child_dirs(&dir)? {
            queue.push_back((child, depth + 1));
        }
    }

    repos.sort_by(|left, right| {
        discovery_name_rank(&left.name)
            .cmp(&discovery_name_rank(&right.name))
            .then_with(|| left.depth.cmp(&right.depth))
            .then_with(|| left.path.cmp(&right.path))
            .then_with(|| left.name.cmp(&right.name))
    });

    Ok(DiscoverReport {
        root,
        max_depth,
        limit,
        dirs_scanned,
        repos_found: repos.len(),
        repos,
    })
}

fn inspect_candidate_repo(dir: &Path, root: &Path, depth: usize) -> Result<Option<DiscoveredRepo>> {
    let git = dir.join(".git").exists();
    let manifests = direct_manifest_files(dir)?;
    if !git && manifests.is_empty() {
        return Ok(None);
    }

    let path = dir
        .canonicalize()
        .with_context(|| format!("canonicalize discovered repo {}", dir.display()))?;
    let name = path
        .file_name()
        .map(|value| value.to_string_lossy().to_string())
        .unwrap_or_else(|| {
            path.strip_prefix(root)
                .unwrap_or(&path)
                .to_string_lossy()
                .to_string()
        });

    Ok(Some(DiscoveredRepo {
        name,
        path,
        depth,
        git,
        manifests,
    }))
}

fn direct_manifest_files(dir: &Path) -> Result<Vec<String>> {
    let mut manifests = Vec::new();
    let Ok(entries) = fs::read_dir(dir) else {
        return Ok(manifests);
    };
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let file_name = entry.file_name().to_string_lossy().to_string();
        if is_manifest_file(&file_name) {
            manifests.push(file_name);
        }
    }
    manifests.sort();
    Ok(manifests)
}

fn sorted_child_dirs(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut children = Vec::new();
    let Ok(entries) = fs::read_dir(dir) else {
        return Ok(children);
    };
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if should_skip_child(&path)? {
            continue;
        }
        children.push(path);
    }
    children.sort_by(|left, right| {
        discovery_path_rank(left)
            .cmp(&discovery_path_rank(right))
            .then_with(|| left.cmp(right))
    });
    Ok(children)
}

fn should_skip_child(path: &Path) -> Result<bool> {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return Ok(true);
    };
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Ok(true);
    }
    Ok(is_ignored(path) || discovery_ignored_name(path))
}

fn should_skip_dir(path: &Path, root: &Path) -> bool {
    path != root && (is_ignored(path) || discovery_ignored_name(path))
}

fn discovery_ignored_name(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
        return false;
    };
    matches!(
        name,
        ".Trash"
            | ".cache"
            | ".cargo"
            | ".rustup"
            | ".npm"
            | ".local"
            | ".bun"
            | ".pyenv"
            | ".rbenv"
            | ".nvm"
            | ".vscode"
            | ".idea"
            | ".DS_Store"
    )
}

fn discovery_path_rank(path: &Path) -> u8 {
    path.file_name()
        .and_then(|value| value.to_str())
        .map(discovery_name_rank)
        .unwrap_or(0)
}

fn discovery_name_rank(name: &str) -> u8 {
    if name.starts_with(".tmp")
        || name.starts_with("tmp-")
        || name.starts_with(".codex")
        || name.starts_with("_worktree")
        || name.contains("worktree")
    {
        3
    } else if looks_like_dated_worktree(name) {
        1
    } else if name.starts_with('.') {
        2
    } else {
        0
    }
}

fn looks_like_dated_worktree(name: &str) -> bool {
    name.as_bytes().windows(6).any(|window| {
        window[0] == b'2'
            && window[1] == b'0'
            && window[2..].iter().all(|byte| byte.is_ascii_digit())
    })
}

fn canonical_or_self(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}
