fn main() {
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc");
    std::env::set_var("PROTOC", protoc);
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/storage_subset.proto"], &["proto"])
        .expect("compile fake GCS proto");
}
