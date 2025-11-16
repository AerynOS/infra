# Infra

[![docs](https://img.shields.io/badge/docs-passing-brightgreen)](https://serpent-os.github.io/infra-test/)

AerynOS service infrastructure

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

The AerynOS infra project is licensed under the MPL-2.0 license.
