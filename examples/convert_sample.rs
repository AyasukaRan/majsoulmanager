//! Converts a directory of raw Mahjong Soul records to mjai JSONL, for
//! eyeballing what a change to the converter did to real games.
//!
//! Not part of the service. It exists because the corpus lives on the
//! deployment host and the only honest way to judge a decoder change is to run
//! it over records the game actually sent.
//!
//! ```text
//! cargo run --example convert_sample -- <input-dir> <output-dir>
//! ```
use std::path::PathBuf;

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let input = PathBuf::from(args.next().expect("input directory"));
    let output = PathBuf::from(args.next().expect("output directory"));
    std::fs::create_dir_all(&output)?;

    let (mut converted, mut failed) = (0usize, 0usize);
    for entry in std::fs::read_dir(&input)? {
        let path = entry?.path();
        if path.extension().and_then(|value| value.to_str()) != Some("pb") {
            continue;
        }
        let raw = std::fs::read(&path)?;
        match mjai_management::majsoul::convert::convert_record_bytes(&raw, None) {
            Ok((_, gzipped)) => {
                let stem = path.file_stem().unwrap_or_default().to_string_lossy();
                std::fs::write(output.join(format!("{stem}.mjson.gz")), gzipped)?;
                converted += 1;
            }
            Err(error) => {
                failed += 1;
                eprintln!("{}: {error:#}", path.display());
            }
        }
    }
    println!("converted {converted}, failed {failed}");
    Ok(())
}
