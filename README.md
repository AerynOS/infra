# Infra

[![docs](https://img.shields.io/badge/docs-passing-brightgreen)](https://serpent-os.github.io/infra-test/)

Our infra is comprised of the Summit, Avalanche and Vessel service components built in Rust:

- **Summit:** Package build-controller, build-orchestrator and build-dashboard. Monitors recipes tree and automatically builds new, incoming recipes once they show up.
- **Avalanche:** Build agent middleware. Takes build orders from Summit and builds them with Boulder on a remote system, sends build logs back in real time to Summit, and reports the build result to Summit at the end of the build.
- **Vessel:** Package repository manager. Summit tells Vessel which packages and other build artefacts to expect from a build task that Avalanche has completed, and then Avalanche pushes those packages and build artefacts to Vessel, which then saves them in the appropriate place and re-indexes the repository with the new packages, so users can install/update them.

## Prerequisites

- Infra can be brought up via `docker-compose`.
- `just` is used as a runner tool to streamline this.

```sh
# on AerynOS
sudo moss sync -u
sudo moss it docker-compose just 'binary(protoc)' -y
sudo usermod -a -G docker "${USER}"
sudo systemctl enable --now docker.socket docker.service
sudo systemctl reboot
```

```sh
# on solus:
eopkg it docker docker-compose just protobuf-devel -y
sudo usermod -a -G docker "${USER}"
sudo systemctl reboot
```

## Testing

```sh
# Will build docker images and bring up `test/docker-compose.yaml`
just up
# Monitor service status and events
just logs
# Shut down containers
just down
```

## Running outside of Docker

Update the `test/*/config.toml` files to point at `localhost` instead of docker container name if
wanting to use those configs.

For testing only the summit frontend, use this command:

    just summit-dev


## License

The AerynOS infra is available under the terms of the [MPL-2.0](https://spdx.org/licenses/MPL-2.0.html) licence.
