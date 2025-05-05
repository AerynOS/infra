#!/usr/bin/env bash

# Crude AerynOS Rust service deployment script

create-service-user () {
  local _svc="$1"
  [[ ! -n "${_svc}" || "${_svc}" = "" ]] && return 1

  # create service users and home dirs
  sudo useradd --create-home --home-dir /srv/${_svc}-rs --user-group --system ${_svc}-rs
  # add invoking user to service groups
  sudo usermod -a -G ${_svc}-rs ${USER}
  echo "Now Relog to ensure that your user is part of the ${_svc}-rs group."
}

deploy-service () {
  local _svc="$1"
  [[ ! -n "${_svc}" || "${_svc}" = "" ]] && return 1
  # necessary for setgid on dirs being inherited by newly created files
  umask 0002
  # set correct permissions for service home dir (o+X for caddy to be able to serve files)
  sudo chmod -Rc u+rwX,g+rwX,o+X /srv/${_svc}-rs
  # set up state dir to be ready for the .privkey private key in bytes format
  mkdir -pv /srv/${_svc}-rs/${_svc}/state
  # copy binaries to service home dirs
  cp -v ../target/release/${_svc} /srv/${_svc}-rs/${_svc}/${_svc}.app
  chmod -c a+x /srv/${_svc}-rs/${_svc}/${_svc}.app
  # reset permissions
  sudo chown -Rc ${_svc}-rs:${_svc}-rs /srv/${_svc}-rs
  sudo chmod -Rc u+rwX,g+rwX,o+X /srv/${_svc}-rs/${_svc}
  # copy .service definitions to /etc/systemd/system/
  sudo cp -v aos-${_svc}-rs.service /etc/systemd/system/
  sudo systemctl daemon-reload
  echo "Now set up config.toml files and private/public keys."
}

deploy-avalanche-service ()
{
  echo -e "\nAllow avalanche-rs user to call /usr/bin/boulder w/no password:"
  echo -e "avalanche-rs ALL = NOPASSWD: /usr/bin/nice, /usr/bin/boulder\n" | sudo tee /etc/sudoers.d/avalanche-rs-boulder
}

reset-service-state () {
  local _svc="$1"
  [[ ! -n "${_svc}" || "${_svc}" = "" ]] && return 1
  local _statedir="/srv/${_svc}-rs/${_svc}/state/"
  [[ -d ${_statedir} ]] && pushd ${_statedir} && rm -rf * && ls -la && popd
  echo "${_svc} state dir reset. NB: The service private key was not deleted."
}

help() {
echo -e "
  Crude deployment procedure:
  - source source-me-to-deploy-rust-services.bash
  - create-service-user <the service> # (one of avalanche|summit|vessel)
  - relog to update service group membership
  - source source-me-to-deploy-rust-services.bash
  - deploy-service <the service>
  - copy private key to /srv/<the service>-rs/<the service>/state/.privkey
  - edit and copy service config.toml to /srv/<the service>-rs/<the service/
    - also copy seed.toml to this dir for summit
  - If you are deploying avalanche, run deploy-avalanche-service to enable
    sudo execution of 'nice -n20 boulder' with no passwd for avalanche-rs
  - To reset the state of a deployed service, run 'reset-service-state <the service>'
    (one of avalanche|summit|vessel)
  - Remember to create an admin.pem private key, and use its public key for the
    public_key field in the [admin] section for summit-rs.
"
}

help
