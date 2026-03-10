fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all("src/generated")?;

    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .out_dir("src/generated")
        .file_descriptor_set_path("src/generated/world_descriptor.bin")
        .compile_protos(
            &[
                "proto/world.proto",
                "proto/types.proto",
                "proto/schema.proto",
            ],
            &["proto"],
        )?;

    println!("cargo:rerun-if-changed=proto/world.proto");
    println!("cargo:rerun-if-changed=proto/types.proto");
    println!("cargo:rerun-if-changed=proto/schema.proto");

    Ok(())
}
