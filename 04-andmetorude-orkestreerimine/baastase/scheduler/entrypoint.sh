#!/bin/sh
# `set -eu` lõpetab skripti kohe, kui mõni käsk ebaõnnestub
# või kui kasutame määramata muutujat.
set -eu

# Loome logikausta ja logifaili ette ära,
# et cron saaks sinna kohe esimese käivituse kirjutada.
mkdir -p /var/log/praktikum
touch /var/log/praktikum/pipeline.log

# Seame konteineri ajavööndi Tallinna ajale.
ln -snf "/usr/share/zoneinfo/${TZ:-Europe/Tallinn}" /etc/localtime
echo "${TZ:-Europe/Tallinn}" > /etc/timezone

# Paigaldame cron'i jaoks selle praktikumi crontab faili.
crontab /app/scheduler/crontab

# Kirjutame logifaili ühe stardirea, et oleks näha:
# scheduler käivitus ja logimine töötab.
echo "[scheduler] Cron käivitus $(date --iso-8601=seconds)" >> /var/log/praktikum/pipeline.log

# `exec` asendab selle shelli cron protsessiga.
# Nii jääb cron konteineri põhikäskluseks.
exec cron -f
