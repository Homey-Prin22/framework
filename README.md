## Overview

This repository contains the server-side infrastructure of the HOMEY project. The project server is deployed using a container-based approach to ensure portability, reproducibility, and ease of installation across different execution environments.
All components are encapsulated in Docker containers and orchestrated through Docker Compose. This approach allows the entire system to be instantiated with minimal manual configuration.

## Infrastructure Prerequisites
The deployment requires the following software components:
•	Docker (version ≥ 24)
•	Docker Compose (v2)

## Container Details
...

## Deployment Procedure
The entire infrastructure can be deployed using a single Docker Compose configuration file.
After copying the example environment file and configuring any environment-specific variables, all containers can be instantiated using standard Docker Compose commands.


```bash
cp .env.example .env
docker compose up -d
