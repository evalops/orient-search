//! Bounded local repository discovery for shard setup.

use crate::repo_index::{is_ignored, is_manifest_file};
use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const GIT_METADATA_TIMEOUT: Duration = Duration::from_millis(250);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoverOptions {
    pub max_depth: usize,
    pub limit: usize,
    pub family_limit: Option<usize>,
    pub git_metadata: bool,
    pub tracked_files: bool,
    pub nested_manifests: bool,
}

impl Default for DiscoverOptions {
    fn default() -> Self {
        Self {
            max_depth: 4,
            limit: 500,
            family_limit: None,
            git_metadata: false,
            tracked_files: false,
            nested_manifests: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoverReport {
    pub root: PathBuf,
    pub max_depth: usize,
    pub limit: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub family_limit: Option<usize>,
    pub dirs_scanned: usize,
    pub candidates_found: usize,
    pub repos_found: usize,
    pub repos: Vec<DiscoveredRepo>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub families: Vec<DiscoveredRepoFamily>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveredRepo {
    pub name: String,
    pub path: PathBuf,
    pub depth: usize,
    pub git: bool,
    pub manifests: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_common_dir: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tracked_files: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveredRepoFamily {
    pub name: String,
    pub checkouts: usize,
    pub worktrees: usize,
    pub clones: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_common_dir: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tracked_files: Option<usize>,
    pub paths: Vec<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoverySelectionSummary {
    pub root: PathBuf,
    pub dirs_scanned: usize,
    pub candidates_found: usize,
    pub selected_repos: usize,
    pub family_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub family_limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub top_families: Vec<DiscoveredRepoFamilySummary>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveredRepoFamilySummary {
    pub name: String,
    pub checkouts: usize,
    pub worktrees: usize,
    pub clones: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
}

pub fn discovery_selection_summary(report: &DiscoverReport) -> DiscoverySelectionSummary {
    DiscoverySelectionSummary {
        root: report.root.clone(),
        dirs_scanned: report.dirs_scanned,
        candidates_found: report.candidates_found,
        selected_repos: report.repos_found,
        family_count: report.families.len(),
        family_limit: report.family_limit,
        top_families: report
            .families
            .iter()
            .take(10)
            .map(|family| DiscoveredRepoFamilySummary {
                name: family.name.clone(),
                checkouts: family.checkouts,
                worktrees: family.worktrees,
                clones: family.clones,
                origin: family.origin.clone(),
            })
            .collect(),
    }
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
    let mut family_counts = HashMap::<String, usize>::new();
    let mut dirs_scanned = 0usize;
    let mut repos = Vec::new();
    let mut discovered_repos = Vec::new();

    while let Some((dir, depth)) = queue.pop_front() {
        let key = canonical_or_self(&dir);
        if !seen_dirs.insert(key) {
            continue;
        }
        if should_skip_dir(&dir, &root) {
            continue;
        }
        dirs_scanned += 1;

        let candidate = inspect_candidate_repo(&dir, &root, depth, options)?;
        let stop_at_git_repo = candidate
            .as_ref()
            .is_some_and(|repo| repo.git && !options.nested_manifests);
        if let Some(repo) = candidate {
            let repo_key = canonical_or_self(&repo.path);
            if seen_repos.insert(repo_key) {
                discovered_repos.push(repo.clone());
                if should_select_repo(&repo, options.family_limit, &mut family_counts) {
                    repos.push(repo);
                    if limit > 0 && repos.len() >= limit {
                        break;
                    }
                }
            }
        }

        if stop_at_git_repo {
            continue;
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
        family_limit: options.family_limit,
        dirs_scanned,
        candidates_found: discovered_repos.len(),
        repos_found: repos.len(),
        families: repo_families(&discovered_repos),
        repos,
    })
}

fn inspect_candidate_repo(
    dir: &Path,
    root: &Path,
    depth: usize,
    options: &DiscoverOptions,
) -> Result<Option<DiscoveredRepo>> {
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

    let metadata = if git && (options.git_metadata || options.family_limit.is_some()) {
        git_metadata_for_repo(&path, options.tracked_files)
    } else {
        RepoGitMetadata::default()
    };

    Ok(Some(DiscoveredRepo {
        name,
        path,
        depth,
        git,
        manifests,
        git_kind: metadata.git_kind,
        branch: metadata.branch,
        origin: metadata.origin,
        git_common_dir: metadata.git_common_dir,
        tracked_files: metadata.tracked_files,
    }))
}

fn should_select_repo(
    repo: &DiscoveredRepo,
    family_limit: Option<usize>,
    family_counts: &mut HashMap<String, usize>,
) -> bool {
    let Some(limit) = family_limit.filter(|limit| *limit > 0) else {
        return true;
    };
    let key = repo_family_key(repo);
    let count = family_counts.entry(key).or_insert(0);
    if *count >= limit {
        return false;
    }
    *count += 1;
    true
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoGitMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_common_dir: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tracked_files: Option<usize>,
}

pub fn git_metadata_for_repo(repo: &Path, include_tracked_files: bool) -> RepoGitMetadata {
    let Some(common_dir) = git_stdout(
        repo,
        &["rev-parse", "--path-format=absolute", "--git-common-dir"],
    )
    .map(PathBuf::from) else {
        return RepoGitMetadata::default();
    };
    let git_kind = if repo.join(".git").is_file() {
        "worktree"
    } else {
        "clone"
    };
    RepoGitMetadata {
        git_kind: Some(git_kind.to_string()),
        branch: git_stdout(repo, &["branch", "--show-current"]),
        origin: git_stdout(repo, &["remote", "get-url", "origin"]),
        git_common_dir: Some(common_dir),
        tracked_files: include_tracked_files
            .then(|| git_tracked_file_count(repo))
            .flatten(),
    }
}

fn git_stdout(repo: &Path, args: &[&str]) -> Option<String> {
    let output = git_output(repo, args)?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8(output.stdout).ok()?;
    let trimmed = text.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

fn git_tracked_file_count(repo: &Path) -> Option<usize> {
    let output = git_output(repo, &["ls-files"])?;
    if !output.status.success() {
        return None;
    }
    Some(output.stdout.iter().filter(|byte| **byte == b'\n').count())
}

fn git_output(repo: &Path, args: &[&str]) -> Option<Output> {
    let mut child = Command::new("git")
        .arg("-c")
        .arg("core.fsmonitor=false")
        .arg("-C")
        .arg(repo)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let started = Instant::now();
    loop {
        match child.try_wait().ok()? {
            Some(_) => return child.wait_with_output().ok(),
            None if started.elapsed() >= GIT_METADATA_TIMEOUT => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
            None => thread::sleep(Duration::from_millis(10)),
        }
    }
}

fn repo_families(repos: &[DiscoveredRepo]) -> Vec<DiscoveredRepoFamily> {
    let mut builders: HashMap<String, DiscoveredRepoFamilyBuilder> = HashMap::new();
    for repo in repos {
        if repo.origin.is_none() && repo.git_common_dir.is_none() {
            continue;
        }
        let key = repo_family_key(repo);
        let builder = builders.entry(key).or_insert_with(|| {
            DiscoveredRepoFamilyBuilder::new(
                repo_family_name(repo),
                repo.origin.clone(),
                repo.git_common_dir.clone(),
            )
        });
        builder.checkouts += 1;
        match repo.git_kind.as_deref() {
            Some("worktree") => builder.worktrees += 1,
            Some("clone") => builder.clones += 1,
            _ => {}
        }
        if let Some(count) = repo.tracked_files {
            *builder.tracked_files.get_or_insert(0) += count;
        }
        builder.paths.push(repo.path.clone());
    }

    let mut families = builders
        .into_values()
        .map(DiscoveredRepoFamilyBuilder::finish)
        .collect::<Vec<_>>();
    families.sort_by(|left, right| {
        right
            .checkouts
            .cmp(&left.checkouts)
            .then_with(|| right.tracked_files.cmp(&left.tracked_files))
            .then_with(|| left.name.cmp(&right.name))
    });
    families
}

fn repo_family_key(repo: &DiscoveredRepo) -> String {
    repo.origin
        .clone()
        .or_else(|| {
            repo.git_common_dir
                .as_ref()
                .map(|path| path.to_string_lossy().to_string())
        })
        .unwrap_or_else(|| repo.name.clone())
}

struct DiscoveredRepoFamilyBuilder {
    name: String,
    checkouts: usize,
    worktrees: usize,
    clones: usize,
    origin: Option<String>,
    git_common_dir: Option<PathBuf>,
    tracked_files: Option<usize>,
    paths: Vec<PathBuf>,
}

impl DiscoveredRepoFamilyBuilder {
    fn new(name: String, origin: Option<String>, git_common_dir: Option<PathBuf>) -> Self {
        Self {
            name,
            checkouts: 0,
            worktrees: 0,
            clones: 0,
            origin,
            git_common_dir,
            tracked_files: None,
            paths: Vec::new(),
        }
    }

    fn finish(mut self) -> DiscoveredRepoFamily {
        self.paths.sort();
        self.paths.dedup();
        DiscoveredRepoFamily {
            name: self.name,
            checkouts: self.checkouts,
            worktrees: self.worktrees,
            clones: self.clones,
            origin: self.origin,
            git_common_dir: self.git_common_dir,
            tracked_files: self.tracked_files,
            paths: self.paths,
        }
    }
}

fn repo_family_name(repo: &DiscoveredRepo) -> String {
    repo.origin
        .as_deref()
        .and_then(origin_repo_name)
        .or_else(|| {
            repo.git_common_dir
                .as_ref()
                .and_then(|path| path.parent())
                .and_then(|path| path.file_name())
                .map(|name| name.to_string_lossy().to_string())
        })
        .unwrap_or_else(|| repo.name.clone())
}

fn origin_repo_name(origin: &str) -> Option<String> {
    let trimmed = origin.trim_end_matches(".git");
    let tail = trimmed.rsplit(['/', ':']).next()?;
    (!tail.is_empty()).then(|| tail.to_string())
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
