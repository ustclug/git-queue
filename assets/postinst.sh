#!/bin/sh
# Modified from https://github.com/caddyserver/dist/blob/master/scripts/postinstall.sh
# Apache License 2.0

set -e

if [ "$1" = "configure" ]; then
	# Add user
	if ! getent passwd git-queue >/dev/null; then
		useradd --system \
			--home-dir /nonexistent \
			--no-create-home \
			--shell /usr/sbin/nologin \
			--comment "git queue manager" \
			git-queue
	fi

	# Add log directory with correct permissions
	mkdir -p /var/log/git-queue
	chown git-queue:adm /var/log/git-queue
	chmod 0750 /var/log/git-queue
fi

if [ "$1" = "configure" ] || [ "$1" = "abort-upgrade" ] || [ "$1" = "abort-deconfigure" ] || [ "$1" = "abort-remove" ] ; then
	# This will only remove masks created by d-s-h on package removal.
	deb-systemd-helper unmask git-queue.service >/dev/null || true

	# was-enabled defaults to true, so new installations run enable.
	if deb-systemd-helper --quiet was-enabled git-queue.service; then
		# Enables the unit on first installation, creates new
		# symlinks on upgrades if the unit file has changed.
		deb-systemd-helper enable git-queue.service >/dev/null || true
		deb-systemd-invoke start git-queue.service >/dev/null || true
	else
		# Update the statefile to add new symlinks (if any), which need to be
		# cleaned up on purge. Also remove old symlinks.
		deb-systemd-helper update-state git-queue.service >/dev/null || true
	fi

	# Restart only if it was already started
	if [ -d /run/systemd/system ]; then
		systemctl --system daemon-reload >/dev/null || true
		if [ -n "$2" ]; then
			deb-systemd-invoke try-restart git-queue.service >/dev/null || true
		fi
	fi
fi
