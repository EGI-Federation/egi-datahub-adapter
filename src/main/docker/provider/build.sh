#!/bin/bash

echo "Building the containers..."
sudo -E docker compose -p euh4d-egi-adapter up -d --build --remove-orphans
