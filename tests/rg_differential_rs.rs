use std::fs;
use std::path::Path;
use std::process::Command;

use orient::repo_index::{SearchFilters, search_repo_fast_filtered};

fn write(path: &Path, text: &str) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, text).unwrap();
}

fn rg_available() -> bool {
    Command::new("rg").arg("--version").output().is_ok()
}

fn rg_content_paths(repo: &Path, literal: &str) -> Vec<String> {
    let output = Command::new("rg")
        .current_dir(repo)
        .args([
            "--files-with-matches",
            "--hidden",
            "--ignore-case",
            "--fixed-strings",
            "--glob",
            "!.git/**",
            "--glob",
            "!.venv/**",
            "--glob",
            "!__pycache__/**",
            "--glob",
            "!.pytest_cache/**",
            "--glob",
            "!.orient/**",
            "--glob",
            "!node_modules/**",
            "--glob",
            "!dist/**",
            "--glob",
            "!build/**",
            "--glob",
            "!.next/**",
            "--glob",
            "!coverage/**",
            "--glob",
            "!target/**",
            literal,
            ".",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "rg failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let mut paths = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| line.trim_start_matches("./").replace('\\', "/"))
        .filter(|path| language_for(path).is_some())
        .collect::<Vec<_>>();
    paths.sort();
    paths
}

fn orient_paths(repo: &Path, query: &str) -> Vec<String> {
    let mut paths = search_repo_fast_filtered(repo, query, 100, &SearchFilters::default())
        .unwrap_or_else(|error| panic!("Orient search failed for {query:?}: {error}"))
        .into_iter()
        .map(|result| result.path)
        .collect::<Vec<_>>();
    paths.sort();
    paths
}

fn language_for(path: &str) -> Option<&'static str> {
    let file_name = Path::new(path).file_name()?.to_str()?;
    if matches!(
        file_name,
        "README" | "Makefile" | "yarn.lock" | "bun.lock" | "bun.lockb"
    ) {
        return Some("text");
    }
    match Path::new(path)
        .extension()?
        .to_str()?
        .to_ascii_lowercase()
        .as_str()
    {
        "py" => Some("python"),
        "rs" => Some("rust"),
        "js" | "jsx" => Some("javascript"),
        "ts" | "tsx" => Some("typescript"),
        "go" => Some("go"),
        "rb" => Some("ruby"),
        "java" => Some("java"),
        "kt" => Some("kotlin"),
        "swift" => Some("swift"),
        "md" => Some("markdown"),
        "toml" => Some("toml"),
        "json" => Some("json"),
        "yaml" | "yml" => Some("yaml"),
        _ => None,
    }
}

fn is_test_path(path: &str) -> bool {
    let path = path.replace('\\', "/").to_ascii_lowercase();
    let mut file_name = path.as_str();
    for part in path.split('/').filter(|part| !part.is_empty()) {
        if matches!(part, "test" | "tests" | "__tests__" | "spec" | "specs") {
            return true;
        }
        file_name = part;
    }
    if file_name.starts_with("test_")
        || file_name.starts_with("tests_")
        || file_name.starts_with("test-")
        || file_name.starts_with("tests-")
        || file_name.starts_with("spec_")
        || file_name.starts_with("specs_")
        || file_name.starts_with("spec-")
        || file_name.starts_with("specs-")
    {
        return true;
    }
    let stem = file_name
        .rsplit_once('.')
        .map(|(stem, _)| stem)
        .unwrap_or(file_name);
    [
        "_test", "_tests", "_spec", "_specs", ".test", ".tests", ".spec", ".specs", "-test",
        "-tests", "-spec", "-specs",
    ]
    .iter()
    .any(|suffix| stem.ends_with(suffix))
}

fn is_generated_path(path: &str) -> bool {
    let path = path.replace('\\', "/").to_ascii_lowercase();
    for part in path.split('/').filter(|part| !part.is_empty()) {
        if matches!(
            part,
            "generated"
                | "__generated__"
                | "gen"
                | "gensrc"
                | "codegen"
                | "autogen"
                | "auto-generated"
        ) {
            return true;
        }
    }
    let file_name = path.rsplit('/').next().unwrap_or(path.as_str());
    let stem = file_name
        .rsplit_once('.')
        .map(|(stem, _)| stem)
        .unwrap_or(file_name);
    stem == "generated"
        || stem.starts_with("generated_")
        || stem.starts_with("generated-")
        || stem.ends_with("_generated")
        || stem.ends_with("-generated")
        || stem.ends_with(".generated")
        || stem.ends_with(".gen")
        || stem.ends_with("_gen")
        || stem.ends_with("-gen")
        || file_name.ends_with(".pb.go")
        || file_name.ends_with(".pb.rs")
        || file_name.ends_with(".g.dart")
}

fn file_name(path: &str) -> String {
    Path::new(path)
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_ascii_lowercase()
}

fn extension(path: &str) -> Option<String> {
    Path::new(path)
        .extension()
        .map(|extension| extension.to_string_lossy().to_ascii_lowercase())
}

#[test]
fn fallback_scoped_search_matches_rg_content_set_for_agent_filters() {
    if !rg_available() {
        return;
    }

    let repo = tempfile::tempdir().unwrap();
    for path in [
        "src/auth.rs",
        "src/generated/cache.rs",
        "src/notes.md",
        "src/auth.ts",
        "src/auth.test.ts",
        "src/api.pb.go",
        "src/generated_client.rs",
        "src/auth_gen.rs",
        "src/models.g.dart",
        "gen/schema.rs",
        "codegen/client.ts",
        "tests/auth_test.rs",
        "spec/gateway_spec.rs",
        "docs/auth.md",
        "scripts/auth.py",
        "README",
        "node_modules/pkg/ignored.rs",
        "dist/ignored.rs",
        "assets/blob.bin",
    ] {
        write(
            &repo.path().join(path),
            &format!("// {path}\nconst MAGICNEEDLE: &str = \"present\";\n"),
        );
    }

    let rg_paths = rg_content_paths(repo.path(), "MAGICNEEDLE");
    let cases: Vec<(&str, Box<dyn Fn(&str) -> bool>)> = vec![
        ("MAGICNEEDLE", Box::new(|_| true)),
        (
            "lang:rust MAGICNEEDLE",
            Box::new(|path| language_for(path) == Some("rust")),
        ),
        ("test:true MAGICNEEDLE", Box::new(|path| is_test_path(path))),
        (
            "test:false MAGICNEEDLE",
            Box::new(|path| !is_test_path(path)),
        ),
        (
            "generated:false MAGICNEEDLE",
            Box::new(|path| !is_generated_path(path)),
        ),
        (
            "path:src MAGICNEEDLE -path:generated",
            Box::new(|path| path.contains("src") && !path.contains("generated")),
        ),
        (
            "file:auth.rs MAGICNEEDLE",
            Box::new(|path| file_name(path).contains("auth.rs")),
        ),
        (
            "file:*.md MAGICNEEDLE",
            Box::new(|path| file_name(path).ends_with(".md")),
        ),
        (
            "ext:ts MAGICNEEDLE",
            Box::new(|path| extension(path).as_deref() == Some("ts")),
        ),
        (
            "-ext:rs MAGICNEEDLE",
            Box::new(|path| extension(path).as_deref() != Some("rs")),
        ),
        (
            "lang:rust -file:*spec.rs MAGICNEEDLE",
            Box::new(|path| {
                language_for(path) == Some("rust") && !file_name(path).ends_with("spec.rs")
            }),
        ),
    ];

    for (query, predicate) in cases {
        let expected = rg_paths
            .iter()
            .filter(|path| predicate(path))
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(orient_paths(repo.path(), query), expected, "{query}");
    }
}
