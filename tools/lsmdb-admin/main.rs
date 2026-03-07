use std::env;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use crc32fast::Hasher;
use lsmdb::config::LsmdbConfig;

const DEFAULT_CONFIG_PATH: &str = "lsmdb.toml";
const DEFAULT_ENGINE_ROOT: &str = "data";
const DEFAULT_OUTPUT_DIR: &str = "diagnostics";
const DEFAULT_MAX_LOG_BYTES: u64 = 10 * 1024 * 1024;
const BUNDLE_MANIFEST_VERSION: u32 = 1;

const USAGE_EXIT_CODE: u8 = 64;
const COMMAND_FAILED_EXIT_CODE: u8 = 2;

fn main() -> ExitCode {
    let args = env::args().skip(1).collect::<Vec<_>>();
    match run(args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(RunError::Usage(message)) => {
            eprintln!("{message}");
            ExitCode::from(USAGE_EXIT_CODE)
        }
        Err(RunError::CommandFailed { command, message }) => {
            eprintln!("{command}.result=failed");
            eprintln!("error={message}");
            ExitCode::from(COMMAND_FAILED_EXIT_CODE)
        }
    }
}

#[derive(Debug)]
enum RunError {
    Usage(String),
    CommandFailed { command: &'static str, message: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Command {
    Help,
    ConfigCheck { config_path: PathBuf },
    DiagnosticsBundle(DiagnosticsBundleArgs),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DiagnosticsBundleArgs {
    config_path: PathBuf,
    engine_root: PathBuf,
    output_dir: PathBuf,
    log_dir: Option<PathBuf>,
    max_log_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct DirStats {
    file_count: u64,
    total_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct StorageSnapshot {
    wal: DirStats,
    sst: DirStats,
    manifest: DirStats,
    total_files: u64,
    total_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct LogCaptureSummary {
    copied_files: u64,
    copied_bytes: u64,
    truncated_files: u64,
    skipped_files: u64,
}

fn run(args: Vec<String>) -> Result<(), RunError> {
    match parse_command(&args)? {
        Command::Help => {
            print_help();
            Ok(())
        }
        Command::ConfigCheck { config_path } => run_config_check(&config_path),
        Command::DiagnosticsBundle(args) => run_diagnostics_bundle(&args),
    }
}

fn parse_command(args: &[String]) -> Result<Command, RunError> {
    if args.is_empty() {
        return Err(RunError::Usage(help_with_error("missing command")));
    }

    if args.len() == 1 && matches!(args[0].as_str(), "--help" | "-h" | "help") {
        return Ok(Command::Help);
    }

    if args.len() >= 2 && args[0] == "config" && args[1] == "check" {
        return parse_config_check_args(&args[2..]);
    }

    if args.len() >= 2 && args[0] == "diagnostics" && args[1] == "bundle" {
        return parse_diagnostics_bundle_args(&args[2..]);
    }

    Err(RunError::Usage(help_with_error(&format!("unknown command: {}", args.join(" ")))))
}

fn parse_config_check_args(args: &[String]) -> Result<Command, RunError> {
    let mut config_path = PathBuf::from(DEFAULT_CONFIG_PATH);
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--config" => {
                index += 1;
                if index >= args.len() {
                    return Err(RunError::Usage(help_with_error("missing value for --config")));
                }
                config_path = PathBuf::from(&args[index]);
            }
            "--help" | "-h" => {
                return Ok(Command::Help);
            }
            unknown => {
                return Err(RunError::Usage(help_with_error(&format!(
                    "unknown option for config check: {unknown}"
                ))));
            }
        }
        index += 1;
    }

    Ok(Command::ConfigCheck { config_path })
}

fn parse_diagnostics_bundle_args(args: &[String]) -> Result<Command, RunError> {
    let mut parsed = DiagnosticsBundleArgs {
        config_path: PathBuf::from(DEFAULT_CONFIG_PATH),
        engine_root: PathBuf::from(DEFAULT_ENGINE_ROOT),
        output_dir: PathBuf::from(DEFAULT_OUTPUT_DIR),
        log_dir: None,
        max_log_bytes: DEFAULT_MAX_LOG_BYTES,
    };

    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--config" => {
                index += 1;
                if index >= args.len() {
                    return Err(RunError::Usage(help_with_error("missing value for --config")));
                }
                parsed.config_path = PathBuf::from(&args[index]);
            }
            "--engine-root" => {
                index += 1;
                if index >= args.len() {
                    return Err(RunError::Usage(help_with_error(
                        "missing value for --engine-root",
                    )));
                }
                parsed.engine_root = PathBuf::from(&args[index]);
            }
            "--output-dir" => {
                index += 1;
                if index >= args.len() {
                    return Err(RunError::Usage(help_with_error("missing value for --output-dir")));
                }
                parsed.output_dir = PathBuf::from(&args[index]);
            }
            "--log-dir" => {
                index += 1;
                if index >= args.len() {
                    return Err(RunError::Usage(help_with_error("missing value for --log-dir")));
                }
                parsed.log_dir = Some(PathBuf::from(&args[index]));
            }
            "--max-log-bytes" => {
                index += 1;
                if index >= args.len() {
                    return Err(RunError::Usage(help_with_error(
                        "missing value for --max-log-bytes",
                    )));
                }
                parsed.max_log_bytes = args[index].parse::<u64>().map_err(|err| {
                    RunError::Usage(help_with_error(&format!(
                        "invalid value for --max-log-bytes: {err}"
                    )))
                })?;
            }
            "--help" | "-h" => {
                return Ok(Command::Help);
            }
            unknown => {
                return Err(RunError::Usage(help_with_error(&format!(
                    "unknown option for diagnostics bundle: {unknown}"
                ))));
            }
        }
        index += 1;
    }

    Ok(Command::DiagnosticsBundle(parsed))
}

fn run_config_check(config_path: &Path) -> Result<(), RunError> {
    let config =
        LsmdbConfig::load_from_path(config_path).map_err(|err| RunError::CommandFailed {
            command: "config.check",
            message: format!("{err} (path={})", config_path.display()),
        })?;

    let diagnostics = config.startup_diagnostics().map_err(|err| RunError::CommandFailed {
        command: "config.check",
        message: format!("{err} (path={})", config_path.display()),
    })?;

    println!("config.check=ok");
    println!("config.path={}", config_path.display());
    for line in diagnostics.as_key_value_lines() {
        println!("{line}");
    }

    Ok(())
}

fn run_diagnostics_bundle(args: &DiagnosticsBundleArgs) -> Result<(), RunError> {
    let generated_at = now_unix_seconds();
    let bundle_id = format!("bundle-{generated_at}-{}", std::process::id());
    let bundle_dir = args.output_dir.join(&bundle_id);

    fs::create_dir_all(&bundle_dir).map_err(|err| RunError::CommandFailed {
        command: "diagnostics.bundle",
        message: format!("failed to create bundle directory '{}': {err}", bundle_dir.display()),
    })?;

    let mut warnings = Vec::new();

    write_lines(
        &bundle_dir.join("build_info.kv"),
        &[
            format!("bundle.generated_unix_seconds={generated_at}"),
            format!("build.version={}", env!("CARGO_PKG_VERSION")),
            format!("build.name={}", env!("CARGO_PKG_NAME")),
            format!("build.target_os={}", env::consts::OS),
            format!("build.target_arch={}", env::consts::ARCH),
        ],
    )
    .map_err(|err| RunError::CommandFailed {
        command: "diagnostics.bundle",
        message: format!("failed to write build info: {err}"),
    })?;

    match fs::read_to_string(&args.config_path) {
        Ok(raw) => {
            let redacted = redact_sensitive_kv_lines(&raw);
            write_string(&bundle_dir.join("config.redacted.toml"), &redacted).map_err(|err| {
                RunError::CommandFailed {
                    command: "diagnostics.bundle",
                    message: format!("failed to write redacted config: {err}"),
                }
            })?;
        }
        Err(err) => warnings.push(format!(
            "config.read_error=failed to read '{}': {err}",
            args.config_path.display()
        )),
    }

    match LsmdbConfig::load_from_path(&args.config_path).and_then(|cfg| cfg.startup_diagnostics()) {
        Ok(diag) => {
            write_lines(&bundle_dir.join("startup_diagnostics.kv"), &diag.as_key_value_lines())
                .map_err(|err| RunError::CommandFailed {
                    command: "diagnostics.bundle",
                    message: format!("failed to write startup diagnostics: {err}"),
                })?;
        }
        Err(err) => warnings.push(format!(
            "config.diagnostics_error={} (path={})",
            err,
            args.config_path.display()
        )),
    }

    let snapshot = collect_storage_snapshot(&args.engine_root, &mut warnings);
    write_lines(
        &bundle_dir.join("storage_snapshot.kv"),
        &[
            format!("engine_root.path={}", args.engine_root.display()),
            format!("storage.wal.file_count={}", snapshot.wal.file_count),
            format!("storage.wal.total_bytes={}", snapshot.wal.total_bytes),
            format!("storage.sst.file_count={}", snapshot.sst.file_count),
            format!("storage.sst.total_bytes={}", snapshot.sst.total_bytes),
            format!("storage.manifest.file_count={}", snapshot.manifest.file_count),
            format!("storage.manifest.total_bytes={}", snapshot.manifest.total_bytes),
            format!("storage.total.file_count={}", snapshot.total_files),
            format!("storage.total.total_bytes={}", snapshot.total_bytes),
        ],
    )
    .map_err(|err| RunError::CommandFailed {
        command: "diagnostics.bundle",
        message: format!("failed to write storage snapshot: {err}"),
    })?;

    let log_summary = match &args.log_dir {
        Some(log_dir) => {
            capture_logs(log_dir, &bundle_dir.join("logs"), args.max_log_bytes, &mut warnings)
                .map_err(|err| RunError::CommandFailed {
                    command: "diagnostics.bundle",
                    message: format!("failed while capturing logs: {err}"),
                })?
        }
        None => LogCaptureSummary::default(),
    };

    write_lines(
        &bundle_dir.join("bundle_manifest.kv"),
        &build_manifest_lines(generated_at, &bundle_id, args, snapshot, log_summary, &warnings),
    )
    .map_err(|err| RunError::CommandFailed {
        command: "diagnostics.bundle",
        message: format!("failed to write bundle manifest: {err}"),
    })?;

    let checksum_path = bundle_dir.join("checksums.crc32");
    write_checksums(&bundle_dir, &checksum_path).map_err(|err| RunError::CommandFailed {
        command: "diagnostics.bundle",
        message: format!("failed to write checksums: {err}"),
    })?;

    println!("diagnostics.bundle=ok");
    println!("diagnostics.bundle_id={bundle_id}");
    println!("diagnostics.output_dir={}", bundle_dir.display());
    println!("diagnostics.warning_count={}", warnings.len());

    Ok(())
}

fn collect_storage_snapshot(engine_root: &Path, warnings: &mut Vec<String>) -> StorageSnapshot {
    let wal = collect_dir_stats(&engine_root.join("wal"), "wal", warnings);
    let sst = collect_dir_stats(&engine_root.join("sst"), "sst", warnings);
    let manifest = collect_dir_stats(&engine_root.join("manifest"), "manifest", warnings);

    StorageSnapshot {
        wal,
        sst,
        manifest,
        total_files: wal.file_count + sst.file_count + manifest.file_count,
        total_bytes: wal.total_bytes + sst.total_bytes + manifest.total_bytes,
    }
}

fn collect_dir_stats(path: &Path, label: &str, warnings: &mut Vec<String>) -> DirStats {
    if !path.exists() {
        warnings.push(format!("storage.{label}.warning=directory not found: {}", path.display()));
        return DirStats::default();
    }

    let files = match list_regular_files(path) {
        Ok(files) => files,
        Err(err) => {
            warnings.push(format!(
                "storage.{label}.warning=failed to list files at '{}': {err}",
                path.display()
            ));
            return DirStats::default();
        }
    };

    let mut stats = DirStats::default();
    for file in files {
        match fs::metadata(&file) {
            Ok(metadata) => {
                stats.file_count = stats.file_count.saturating_add(1);
                stats.total_bytes = stats.total_bytes.saturating_add(metadata.len());
            }
            Err(err) => warnings.push(format!(
                "storage.{label}.warning=failed to stat '{}': {err}",
                file.display()
            )),
        }
    }

    stats
}

fn capture_logs(
    log_dir: &Path,
    output_logs_dir: &Path,
    max_log_bytes: u64,
    warnings: &mut Vec<String>,
) -> std::io::Result<LogCaptureSummary> {
    if !log_dir.exists() {
        warnings.push(format!("logs.warning=directory not found: {}", log_dir.display()));
        return Ok(LogCaptureSummary::default());
    }

    let mut files = list_regular_files(log_dir)?;
    files.sort_by(|a, b| {
        let a_time = fs::metadata(a).and_then(|m| m.modified()).ok();
        let b_time = fs::metadata(b).and_then(|m| m.modified()).ok();
        b_time.cmp(&a_time)
    });

    let mut summary = LogCaptureSummary::default();
    let mut remaining = max_log_bytes;

    for source_path in files {
        if remaining == 0 {
            break;
        }

        let raw = match fs::read(&source_path) {
            Ok(raw) => raw,
            Err(_) => {
                summary.skipped_files = summary.skipped_files.saturating_add(1);
                continue;
            }
        };

        let to_copy = remaining.min(raw.len() as u64) as usize;
        if to_copy == 0 {
            break;
        }

        let clipped = &raw[..to_copy];
        let redacted = redact_sensitive_kv_lines(&String::from_utf8_lossy(clipped));

        let relative =
            source_path.strip_prefix(log_dir).unwrap_or(source_path.as_path()).to_path_buf();
        let destination = output_logs_dir.join(relative);
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(destination, redacted.as_bytes())?;

        summary.copied_files = summary.copied_files.saturating_add(1);
        summary.copied_bytes = summary.copied_bytes.saturating_add(to_copy as u64);
        if raw.len() > to_copy {
            summary.truncated_files = summary.truncated_files.saturating_add(1);
        }

        remaining = remaining.saturating_sub(to_copy as u64);
    }

    Ok(summary)
}

fn build_manifest_lines(
    generated_at: u64,
    bundle_id: &str,
    args: &DiagnosticsBundleArgs,
    snapshot: StorageSnapshot,
    log_summary: LogCaptureSummary,
    warnings: &[String],
) -> Vec<String> {
    let mut lines = vec![
        format!("manifest.version={BUNDLE_MANIFEST_VERSION}"),
        format!("bundle.id={bundle_id}"),
        format!("bundle.generated_unix_seconds={generated_at}"),
        format!("bundle.tool=lsmdb-admin"),
        format!("bundle.tool_version={}", env!("CARGO_PKG_VERSION")),
        format!("bundle.config_path={}", args.config_path.display()),
        format!("bundle.engine_root={}", args.engine_root.display()),
        format!("bundle.output_dir={}", args.output_dir.display()),
        format!("bundle.max_log_bytes={}", args.max_log_bytes),
        format!(
            "bundle.log_dir={}",
            args.log_dir
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "<none>".to_string())
        ),
        format!("storage.total_files={}", snapshot.total_files),
        format!("storage.total_bytes={}", snapshot.total_bytes),
        format!("logs.copied_files={}", log_summary.copied_files),
        format!("logs.copied_bytes={}", log_summary.copied_bytes),
        format!("logs.truncated_files={}", log_summary.truncated_files),
        format!("logs.skipped_files={}", log_summary.skipped_files),
        format!("warnings.count={}", warnings.len()),
    ];

    for (index, warning) in warnings.iter().enumerate() {
        lines.push(format!("warnings.{index}={warning}"));
    }

    lines
}

fn write_checksums(bundle_dir: &Path, checksum_path: &Path) -> std::io::Result<()> {
    let mut files = list_regular_files(bundle_dir)?;
    files.sort();

    let mut output = String::new();
    for path in files {
        if path == checksum_path {
            continue;
        }
        let checksum = crc32_for_file(&path)?;
        let relative = path.strip_prefix(bundle_dir).unwrap_or(path.as_path());
        output.push_str(&format!("{checksum:08x}  {}\n", relative.display()));
    }

    write_string(checksum_path, &output)
}

fn crc32_for_file(path: &Path) -> std::io::Result<u32> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Hasher::new();
    let mut buffer = [0_u8; 8192];

    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }

    Ok(hasher.finalize())
}

fn list_regular_files(root: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let mut stack = vec![root.to_path_buf()];

    while let Some(path) = stack.pop() {
        let metadata = fs::metadata(&path)?;
        if metadata.is_file() {
            files.push(path);
            continue;
        }

        for entry in fs::read_dir(&path)? {
            let entry = entry?;
            stack.push(entry.path());
        }
    }

    Ok(files)
}

fn write_lines(path: &Path, lines: &[String]) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut file = fs::File::create(path)?;
    for line in lines {
        writeln!(file, "{line}")?;
    }

    Ok(())
}

fn write_string(path: &Path, content: &str) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, content)
}

fn now_unix_seconds() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

fn redact_sensitive_kv_lines(raw: &str) -> String {
    let mut output = String::new();
    for (index, line) in raw.lines().enumerate() {
        if index > 0 {
            output.push('\n');
        }
        output.push_str(&redact_line(line));
    }
    if raw.ends_with('\n') {
        output.push('\n');
    }
    output
}

fn redact_line(line: &str) -> String {
    if line.trim_start().starts_with('#') {
        return line.to_string();
    }

    if let Some(index) = line.find('=') {
        let key = line[..index].trim().to_ascii_lowercase();
        if is_sensitive_key(&key) {
            return format!("{} <redacted>", &line[..=index]);
        }
    }

    if let Some(index) = line.find(':') {
        let key = line[..index].trim().to_ascii_lowercase();
        if is_sensitive_key(&key) {
            return format!("{} <redacted>", &line[..=index]);
        }
    }

    line.to_string()
}

fn is_sensitive_key(key: &str) -> bool {
    const KEYWORDS: [&str; 7] =
        ["password", "secret", "token", "credential", "api_key", "private_key", "access_key"];

    KEYWORDS.iter().any(|needle| key.contains(needle))
}

fn print_help() {
    println!("lsmdb-admin: operational commands for lsmdb");
    println!();
    println!("Usage:");
    println!("  lsmdb-admin config check [--config PATH]");
    println!(
        "  lsmdb-admin diagnostics bundle [--config PATH] [--engine-root PATH] [--output-dir PATH] [--log-dir PATH] [--max-log-bytes BYTES]"
    );
    println!();
    println!("Commands:");
    println!("  config check           Validate config and print startup diagnostics");
    println!("  diagnostics bundle     Build support bundle with redacted config and checksums");
    println!();
    println!("Options:");
    println!("  --config PATH          Config file path (default: {DEFAULT_CONFIG_PATH})");
    println!("  --engine-root PATH     Engine data root (default: {DEFAULT_ENGINE_ROOT})");
    println!("  --output-dir PATH      Bundle output root (default: {DEFAULT_OUTPUT_DIR})");
    println!("  --log-dir PATH         Optional log directory to include");
    println!(
        "  --max-log-bytes BYTES  Max bytes copied from logs (default: {DEFAULT_MAX_LOG_BYTES})"
    );
    println!("  --help, -h             Show help");
    println!();
    println!("Exit codes:");
    println!("  0  success");
    println!("  2  command failed");
    println!("  64 usage error");
}

fn help_with_error(message: &str) -> String {
    format!(
        "error: {message}\n\nUsage:\n  lsmdb-admin config check [--config PATH]\n  lsmdb-admin diagnostics bundle [--config PATH] [--engine-root PATH] [--output-dir PATH] [--log-dir PATH] [--max-log-bytes BYTES]\n  lsmdb-admin --help"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Command, RunError> {
        let values = args.iter().map(|value| (*value).to_string()).collect::<Vec<_>>();
        parse_command(&values)
    }

    #[test]
    fn parses_config_check_with_default_path() {
        let command = parse(&["config", "check"]).expect("parse default config command");
        assert_eq!(
            command,
            Command::ConfigCheck { config_path: PathBuf::from(DEFAULT_CONFIG_PATH) }
        );
    }

    #[test]
    fn parses_diagnostics_bundle_with_defaults() {
        let command = parse(&["diagnostics", "bundle"]).expect("parse diagnostics bundle command");
        assert_eq!(
            command,
            Command::DiagnosticsBundle(DiagnosticsBundleArgs {
                config_path: PathBuf::from(DEFAULT_CONFIG_PATH),
                engine_root: PathBuf::from(DEFAULT_ENGINE_ROOT),
                output_dir: PathBuf::from(DEFAULT_OUTPUT_DIR),
                log_dir: None,
                max_log_bytes: DEFAULT_MAX_LOG_BYTES,
            })
        );
    }

    #[test]
    fn parses_diagnostics_bundle_with_custom_args() {
        let command = parse(&[
            "diagnostics",
            "bundle",
            "--config",
            "./dev.toml",
            "--engine-root",
            "./data-dev",
            "--output-dir",
            "./diag",
            "--log-dir",
            "./logs",
            "--max-log-bytes",
            "2048",
        ])
        .expect("parse diagnostics bundle with options");

        assert_eq!(
            command,
            Command::DiagnosticsBundle(DiagnosticsBundleArgs {
                config_path: PathBuf::from("./dev.toml"),
                engine_root: PathBuf::from("./data-dev"),
                output_dir: PathBuf::from("./diag"),
                log_dir: Some(PathBuf::from("./logs")),
                max_log_bytes: 2048,
            })
        );
    }

    #[test]
    fn rejects_unknown_option_for_diagnostics_bundle() {
        let err =
            parse(&["diagnostics", "bundle", "--bogus"]).expect_err("unknown option should fail");
        match err {
            RunError::Usage(message) => {
                assert!(message.contains("unknown option"));
            }
            other => panic!("expected usage error, got {other:?}"),
        }
    }

    #[test]
    fn redacts_sensitive_key_value_lines() {
        let input = "db_password = \"abc\"\nwal.segment_size_bytes = 4096\napi_key: secret\n";
        let output = redact_sensitive_kv_lines(input);

        assert!(output.contains("db_password = <redacted>"));
        assert!(output.contains("api_key: <redacted>"));
        assert!(output.contains("wal.segment_size_bytes = 4096"));
    }
}
