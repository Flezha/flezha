//! Writes the prost module for `runner_commands.proto` into the directory named
//! by the one argument. Run through `protobufs/regen.sh`.

use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = std::env::args().nth(1).ok_or("usage: prost_gen OUT_DIR")?;
    let proto_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let mut config = prost_build::Config::new();
    config.out_dir(out_dir);
    // The connect frame writes these as JSON under their field names.
    for message in ["ApiEntry", "RelayGraph", "GraphChannel", "GraphTasklet"] {
        config.type_attribute(
            format!(".runner_commands.{message}"),
            "#[derive(serde::Serialize)]",
        );
    }
    config.compile_protos(&[proto_dir.join("runner_commands.proto")], &[proto_dir])?;
    Ok(())
}
