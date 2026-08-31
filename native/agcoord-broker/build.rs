use std::env;

fn main() {
    let build_id = env::var("AGCOORD_BUILD_ID").unwrap_or_else(|_| "development".to_owned());
    assert!(
        build_id == "development"
            || build_id
                .strip_prefix("sha256:")
                .is_some_and(|digest| digest.len() == 64
                    && digest.bytes().all(|byte| byte.is_ascii_hexdigit())),
        "AGCOORD_BUILD_ID must be development or sha256:<64 lowercase hexadecimal digits>"
    );
    assert!(
        !build_id.bytes().any(|byte| byte.is_ascii_uppercase()),
        "AGCOORD_BUILD_ID uses lowercase hexadecimal"
    );

    println!("cargo:rerun-if-env-changed=AGCOORD_BUILD_ID");
    println!(
        "cargo:rustc-env=AGCOORD_TARGET={}",
        env::var("TARGET").unwrap()
    );
    println!("cargo:rustc-env=AGCOORD_BUILD_ID={build_id}");
}
