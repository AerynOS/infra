#!/usr/bin/env bash

# Crude AerynOS Rust service deployment script

create-service-user () {
  local _svc="$1"
  [[ ! -n "${_svc}" || "${_svc}" = "" ]] && return 1
  # create a switch statement here?

  # create service users and home dirs
  sudo useradd --create-home --home-dir /srv/${_svc}-rs --user-group --system --add-subids-for-system ${_svc}-rs
  # add invoking user to service groups for sysadmin convenience purposes
  sudo usermod -a -G ${_svc}-rs ${USER}
  echo -e "\nNow Relog to ensure that your user is part of the ${_svc}-rs group.\n"
}

deploy-avalanche-service ()
{
#  echo -e "\nAllow avalanche-rs user to call /usr/bin/boulder w/no password:"
#  echo "avalanche-rs ALL = NOPASSWD: /usr/bin/nice, /usr/bin/boulder" | sudo tee /etc/sudoers.d/avalanche-rs-boulder
#  sudo ls -laF /etc/sudoers.d/avalanche-rs-boulder
  [[ -d /srv/avalanche-rs/.cache/boulder ]] \
  && echo -e "\nCleaning out avalanche-rs user boulder cache..." \
  && sudo rm -rf /srv/avalanche-rs/.cache/boulder
  [[ -d /srv/avalanche-rs/.cache/moss ]] \
  && echo -e "\nCleaning out avalanche-rs user moss cache..." \
  && sudo rm -rf /srv/avalanche-rs/.cache/moss
}

deploy-vessel-service ()
{
  # ensure an empty import/ dir is created
  [[ ! -d /srv/vessel-rs/import ]] \
  && echo -e "\nCreating /srv/vessel-rs/import/ dir for preseeding .stones..." \
  && mkdir -pv /srv/vessel-rs/import/
}

deploy-service () {
  local _svc="$1"
  [[ ! -n "${_svc}" || "${_svc}" = "" ]] && return 1
  # necessary for setgid on dirs being inherited by newly created files
  umask 0002
  # set correct permissions for service home dir (o+X for caddy to be able to serve files)
  sudo chmod -c u+rwX,g+rwX,o+X /srv/${_svc}-rs
  # set up state dir to be ready for the .privkey private key in bytes format
  sudo mkdir -pv /srv/${_svc}-rs/${_svc}/state
  # copy binaries to service home dirs
  sudo cp -v ../target/infratest/${_svc} /srv/${_svc}-rs/${_svc}/${_svc}.app
  sudo chmod -c a+x /srv/${_svc}-rs/${_svc}/${_svc}.app
  # delete boulder + moss cache if we're resetting avalanche
  [[ "${_svc}" == "avalanche" ]] && deploy-avalanche-service
  # ensure that the vessel import dir always exists
  [[ "${_svc}" == "vessel" ]] && deploy-vessel-service
  # reset permissions
  sudo chown -Rc ${_svc}-rs:${_svc}-rs /srv/${_svc}-rs
  sudo chmod -Rc u+rwX,g+rwX,o+X /srv/${_svc}-rs/${_svc}
  ls -laF /srv/${_svc}-rs/${_svc}
  # copy .service definitions to /etc/systemd/system/
  sudo cp -v aos-${_svc}-rs.service /etc/systemd/system/
  ls -la /etc/systemd/system/aos-${_svc}-rs.service
  sudo systemctl daemon-reload

  echo -e "\nDid you remember to set up config.toml files and private/public keys?\n"
}


reset-service-state () {
  local _svc="$1"
  [[ ! -n "${_svc}" || "${_svc}" = "" ]] && return 1
  local _statedir="/srv/${_svc}-rs/${_svc}/state/"
  [[ -d ${_statedir} ]] && pushd ${_statedir} && rm -rf * && popd
  local _assetsdir="/srv/${_svc}-rs/${_svc}/assets/"
  [[ -d ${_assetsdir} ]] && pushd ${_assetsdir}/.. && rm -rf assets && popd

  tree -L 3 -apugDF --dirsfirst /srv/${_svc}-rs/
  echo -e "\n${_svc} state dir reset. NB: The service private key was not deleted.\n"
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
  - edit and copy service config.toml to /srv/<the service>-rs/<the service>/
    - also copy seed.toml to this dir for summit
  - To reset the state of a deployed service, run 'reset-service-state <the service>'
    (one of avalanche|summit|vessel)
  - Remember to create an admin.pem private key, and use its public key for the
    public_key field in the [admin] section for services (particularly summit).
"
}

help
