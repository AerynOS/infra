# Deployment notes for AerynOS infra

This document is intended to give a rough outline of the necessary steps to manually
deploy AerynOS infra on a bare metal Linux install.

The documentation assumes that the summit-rs and vessel-rs instances run on the host `aosinfra.lan`. It also
assumes that binaries have been built and are available in the `../target/infratest/` folder relative to this document.

This hostname should obviously be changed to whatever fully qualified domain or IP is used by deployments.



## High level flow

- Create keys in byte form
- Capture public keys from byte form in a single document for use in registering services
- Create directory trees
- Copy .privkey byte format private keys into the relevant `<service>/state/` directories
- Pre-seed keys in config.toml files for each service
- Adjust hostnames and ports in configs and .service definitions
- Deploy config.toml for all services to the `/srv/<service>-rs/<service>/` dirs
- Deploy seed.toml for summit to `/srv/summit-rs/summit/`
- Copy systemd .service definitions to `/etc/systemd/system/` and run `systemctl daemon-reload`
- (Optional) Punch holes in the system firewall to match the ports listed in .service definitions
- Start the aos-summit-rs service and check that it works
- Start aos-vessel-rs service and check that it works
- Add any seed .stones for vessel to the `/srv/vessel-rs/import/` folder
- Adjust the aos-vessel-rs .service definition (already contains a commented out --import example)
- Adjust caddy configuration for summit reverse proxy to status page and vessel repository contents
- Start caddy and check that it serves summit and vessel routes correctly
- Add aos-avalanche-rs services from the hosts you want to use as builders

## How do I remove accounts from summit if I want to reset the associated service installs?

ssh <the server>
cd /srv/summit-rs/summit/state/db/
sudo -u summit-rs sqlite3 service
delete FROM account WHERE public_key IN ('<the public_key>',...);
-- this will also autodelete the associated account_token and endpoint table entries
   that match the rows with the removed account_id's.
ssh <the server running the service you want to reset>
sudo systemctl stop <the service>
cd repos/aos/infra/deployment
source source-me(...)
cd ..
reset-then-deploy <the service>
sudo systemctl start <the service>
