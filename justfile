export PODMAN_COMPOSE_PROVIDER := "podman-compose"
export PODMAN_COMPOSE_WARNING_LOGS := "false"
export COMPOSE_FILE := "./test/compose.yaml"

[private]
help:
	@just --list

[private]
podman-build target profile *ARGS:
	@podman build . -t serpentos/{{target}}:{{profile}} --target {{target}} --build-arg RUST_PROFILE={{profile}} {{ARGS}}

# Build podman containers
build profile="infratest" *ARGS: (podman-build "summit" profile ARGS) (podman-build "avalanche" profile ARGS) (podman-build "vessel" profile ARGS)

# Bring up podman containers
up *ARGS: (_up "infratest" ARGS)

# Bring up podman containers in release mode
up-release *ARGS: (_up "release" ARGS)

_up profile *ARGS:
	RUST_PROFILE={{profile}} podman compose up -d {{ARGS}}

# Follow logs of podman containers
logs *ARGS:
	podman compose logs --follow {{ARGS}}

# Restart podman containers
restart *ARGS:
	podman compose restart {{ARGS}}

# Stop podman containers
stop *ARGS:
	podman compose stop {{ARGS}}

# Bring down podman containers
down *ARGS:
	podman compose down {{ARGS}}

# Quickly view summit front-end changes (DX feature)
summit-dev *ARGS:
	cargo run -p summit --no-default-features --features templates-autoreload -- \
		-c ./test/summit/config.toml \
		--root $(mktemp -d) \
		--static ./crates/summit/static \
		--use-mock-data \
		{{ARGS}}

# Do a fresh build of 'avalanche|summit|vessel', stop it, reset it, deploy it, and start it.
reset-then-deploy *ARGS:
	#!/usr/bin/bash
	cargo build --profile infratest -p {{ARGS}} \
	&& pushd deployment \
	&& source source-me-to-deploy-rust-services.bash >/dev/null \
	&& { sudo systemctl stop aos-{{ARGS}}-rs || true ; } \
	&& reset-service-state {{ARGS}} \
	&& deploy-service {{ARGS}} \
	&& echo -e "Run 'sudo systemctl start aos-{{ARGS}}-rs' to start {{ARGS}}\n"  \
	&& popd
