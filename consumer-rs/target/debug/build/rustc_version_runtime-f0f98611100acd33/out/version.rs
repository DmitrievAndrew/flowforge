
            /// Returns the `rustc` SemVer version and additional metadata
            /// like the git short hash and build date.
            pub fn version_meta() -> VersionMeta {
                VersionMeta {
                    semver: Version {
                        major: 1,
                        minor: 98,
                        patch: 1,
                        pre: Prerelease::new("").unwrap(),
                        build: BuildMetadata::new("").unwrap(),
                    },
                    host: "x86_64-unknown-linux-gnu".to_owned(),
                    short_version_string: "rustc 1.98.1 (48a229cea 2026-09-01) (Arch Linux rust 1:1.98.1-1)".to_owned(),
                    commit_hash: Some("48a229ceaefd4985c50990b14116b6d856af0985".to_owned()),
                    commit_date: Some("2026-09-01".to_owned()),
                    build_date: None,
                    channel: Channel::Stable,
                    llvm_version: Some(LlvmVersion{ major: 22, minor: 1 }),
                }
            }
            