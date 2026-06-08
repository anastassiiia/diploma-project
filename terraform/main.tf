resource "google_compute_network" "vpc_network" {
  name                    = "diploma-vpc"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "subnet" {
  name          = "diploma-subnet"
  region        = "us-central1"
  network       = google_compute_network.vpc_network.id
  ip_cidr_range = "10.0.0.0/24"
}

resource "google_container_cluster" "gke_cluster" {
  name                = "diploma-cluster"
  location            = "us-central1-a"
  deletion_protection = false

  network    = google_compute_network.vpc_network.id
  subnetwork = google_compute_subnetwork.subnet.id

  remove_default_node_pool = true
  initial_node_count       = 1

  monitoring_service = "monitoring.googleapis.com/kubernetes"
  logging_service    = "logging.googleapis.com/kubernetes"
}

resource "google_container_node_pool" "primary_nodes" {
  name     = "diploma-node-pool"
  location = "us-central1-a"
  cluster  = google_container_cluster.gke_cluster.name

  initial_node_count = 2

  autoscaling {
    min_node_count = 2
    max_node_count = 10
  }

  node_config {
    machine_type = "e2-medium"

    oauth_scopes = [
      "https://www.googleapis.com/auth/logging.write",
      "https://www.googleapis.com/auth/monitoring",
      "https://www.googleapis.com/auth/devstorage.read_only",
    ]

    labels = {
      role = "general"
    }
  }
}

resource "google_artifact_registry_repository" "diploma_repo" {
  location      = "us-central1"
  repository_id = "diploma-images"
  description   = "Docker registry for diploma project images"
  format        = "DOCKER"
}
