use std::{env, fs, path::PathBuf};

use prost_build::ServiceGenerator;
use prost_reflect::{DescriptorPool, prost::Message};
use prost_types::FileDescriptorSet;

const PROTO_FILES: &[&str] = &[
    "proto/account.proto",
    "proto/avalanche.proto",
    "proto/collectable.proto",
    "proto/endpoint.proto",
    "proto/remote.proto",
    "proto/summit.proto",
    "proto/vessel.proto",
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tonic_generator = tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .include_file("proto.rs")
        .service_generator();

    let fd_path = PathBuf::from(env::var("OUT_DIR")?).join("file_descriptor_set.bin");

    prost_build::Config::new()
        .file_descriptor_set_path(&fd_path)
        .load_fds(PROTO_FILES, &["proto/"])?;

    let fd_bytes = fs::read(&fd_path)?;
    let fd = FileDescriptorSet::decode(fd_bytes.as_slice())?;
    let dp = DescriptorPool::decode(fd_bytes.as_slice())?;

    prost_build::Config::new()
        .include_file("proto.rs")
        .service_generator(Box::new(Generator {
            tonic: tonic_generator,
            dp,
        }))
        .compile_fds(fd)?;

    Ok(())
}

struct Generator {
    tonic: Box<dyn ServiceGenerator>,
    dp: DescriptorPool,
}

impl ServiceGenerator for Generator {
    fn generate(&mut self, service: prost_build::Service, buf: &mut String) {
        let mut methods = vec![];

        for method in &service.methods {
            let sd = self
                .dp
                .get_service_by_name(&format!("{}.{}", service.package, service.proto_name))
                .expect("missing service descriptor");
            let md = sd
                .methods()
                .find(|m| m.name() == method.proto_name)
                .expect("missing method descriptor");
            let od = md.options();
            let flags = od
                .extensions()
                .find_map(|(a, b)| (a.name() == "flags").then_some(b))
                .and_then(|v| v.as_str())
                .unwrap_or_else(|| {
                    panic!(
                        "missing (options.flags) for {}.{}/{}",
                        service.package, service.proto_name, method.proto_name
                    )
                });

            methods.push((method.clone(), flags.to_owned()));
        }

        buf.push_str("#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]");
        buf.push_str("pub enum Method {");
        for (method, _) in &methods {
            buf.push_str(&format!("\n{},", &method.proto_name));
        }
        buf.push_str("\n}");

        buf.push_str("impl Method {");
        buf.push_str("pub fn from_path(path: &str) -> Option<Self> {");
        buf.push_str("\nmatch path {");
        for (method, _) in &methods {
            buf.push_str(&format!(
                "\n\"/{}.{}/{}\" => Some(Self::{}),",
                service.package, service.proto_name, &method.proto_name, &method.proto_name
            ));
        }
        buf.push_str("_ => None,");
        buf.push('}');
        buf.push_str("\n}");
        buf.push_str("pub fn flags(self) -> Flags {");
        buf.push_str("\nmatch self {");
        for (method, flags) in &methods {
            let flags = flags
                .split(" | ")
                .map(|f| format!("Flags::{f}"))
                .collect::<Vec<_>>()
                .join(" | ");

            buf.push_str(&format!("\nSelf::{} => {flags},", &method.proto_name,));
        }
        buf.push('}');
        buf.push_str("\n}");
        buf.push_str("\n}");

        self.tonic.generate(service, buf);
    }

    fn finalize_package(&mut self, package: &str, buf: &mut String) {
        buf.push_str("use service_core::auth::Flags;");

        self.tonic.finalize_package(package, buf);
    }

    fn finalize(&mut self, buf: &mut String) {
        self.tonic.finalize(buf);
    }
}
