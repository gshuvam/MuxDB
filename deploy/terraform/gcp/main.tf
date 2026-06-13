provider "google" {
  project = var.gcp_project
  region  = var.gcp_region
}

variable "gcp_project" {
  type = string
}

variable "gcp_region" {
  default = "us-central1"
}

# Network
resource "google_compute_network" "vpc_network" {
  name = "muxdb-network"
}

# Firewall for gRPC control plane
resource "google_compute_firewall" "default" {
  name    = "muxdb-firewall"
  network = google_compute_network.vpc_network.name

  allow {
    protocol = "tcp"
    ports    = ["50051"]
  }

  source_ranges = ["0.0.0.0/0"]
}

# VM Instance for MuxDB Control
resource "google_compute_instance" "vm_instance" {
  name         = "muxdb-control-plane"
  machine_type = "e2-medium"
  zone         = "us-central1-a"

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"
    }
  }

  network_interface {
    network = google_compute_network.vpc_network.name
  }
}

# Cloud SQL instances for PostgreSQL Shards
resource "google_sql_database_instance" "postgres_shards" {
  count            = 2
  name             = "muxdb-postgres-shard-${count.index}"
  database_version = "POSTGRES_15"
  region           = var.gcp_region

  settings {
    tier = "db-f1-micro"
  }
}

# Memorystore Redis Instance for cache layer
resource "google_redis_instance" "redis_cache" {
  name           = "muxdb-redis"
  tier           = "BASIC"
  memory_size_gb = 1
  region         = var.gcp_region
}
