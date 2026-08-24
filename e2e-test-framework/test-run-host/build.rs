fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile Drasi protobuf files
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &[
                "proto/drasi/v1/common.proto",
                "proto/drasi/v1/source.proto",
                "proto/drasi/v1/reaction.proto",
            ],
            &["proto"],
        )?;
    Ok(())
}
