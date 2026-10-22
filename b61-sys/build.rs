use std::{env, path::Path};

fn main() {
    let os = env::var("CARGO_CFG_TARGET_OS").unwrap();
    if os == "linux"
        && pkg_config::Config::new()
            .atleast_version("0.1.0")
            .probe("libaribb61")
            .is_ok()
    {
        return;
    }

    let source = Path::new("externals/libaribb61");
    assert!(
        source.join("CMakeLists.txt").exists(),
        "libaribb61 submodule is missing; run git submodule update --init --recursive"
    );
    println!("cargo:rerun-if-changed=externals/libaribb61");
    let mut config = cmake::Config::new(source);
    config
        .define("BUILD_SHARED_LIBS", "OFF")
        .define("BUILD_TESTING", "OFF")
        .define("CMAKE_INSTALL_LIBDIR", "lib");
    if os == "windows"
        && env::var("CARGO_CFG_TARGET_FEATURE")
            .unwrap_or_default()
            .contains("crt-static")
    {
        config.define("CMAKE_MSVC_RUNTIME_LIBRARY", "MultiThreaded");
    }
    let installed = config.build();
    println!("cargo:rustc-link-search=native={}/lib", installed.display());
    println!("cargo:rustc-link-lib=static=aribb61");

    let target = env::var("TARGET").unwrap();
    match os.as_str() {
        "macos" => {
            println!("cargo:rustc-link-lib=framework=PCSC");
            println!("cargo:rustc-link-lib=dylib=c++");
        }
        "linux" => {
            pkg_config::Config::new()
                .probe("libpcsclite")
                .expect("PC/SC development files are required");
            println!("cargo:rustc-link-lib=dylib=stdc++");
            println!("cargo:rustc-link-lib=pthread");
        }
        "windows" => {
            println!("cargo:rustc-link-lib=winscard");
            println!("cargo:rustc-link-lib=ws2_32");
            if target.ends_with("-gnullvm") {
                println!("cargo:rustc-link-lib=dylib=c++");
            } else if target.ends_with("-gnu") {
                println!("cargo:rustc-link-lib=dylib=stdc++");
            }
        }
        _ => panic!("Unsupported libaribb61 target: {target}"),
    }
}
