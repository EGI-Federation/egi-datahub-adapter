#!/bin/bash

echo "Building consumer containers..."
sudo -E docker compose -p euh4d-egi-test up -d --build --remove-orphans
