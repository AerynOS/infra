export COMPOSE_FILE := "./test/docker-compose.yaml"

[private]
help:
	@just --list

[private]
docker-build target profile:
	@docker build . -t serpentos/{{target}}:{{profile}} --target {{target}} --build-arg RUST_PROFILE={{profile}}

# Build docker containers
build profile="dev": (docker-build "summit" profile) (docker-build "avalanche" profile) (docker-build "vessel" profile)

# Bring up docker containers
up *ARGS: (_up "dev" ARGS)

# Bring up docker containers in release mode
up-release *ARGS: (_up "release" ARGS)

_up profile *ARGS: (build profile)
	RUST_PROFILE={{profile}} docker compose up --wait {{ARGS}}

# Follow logs of docker containers
logs *ARGS:
	docker compose logs --follow {{ARGS}}

# Restart docker containers
restart *ARGS:
	docker compose restart {{ARGS}}

# Bring down docker containers
down *ARGS:
	docker compose down -v {{ARGS}}

# Quickly view summit front-end changes (DX feature)
summit-dev *ARGS:
	cargo run -p summit --no-default-features --features templates-autoreload -- -c ./test/summit/config.toml --root $(mktemp -d) --static ./crates/summit/static {{ARGS}}

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
