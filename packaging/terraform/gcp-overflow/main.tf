# A GCP project for AGCoord overflow executors: the place where a placed job's image is
# stored and where its machine runs.  Nothing here runs or bills by itself; machines exist
# only while an executor runs a job.

locals {
  services = toset([
    "artifactregistry.googleapis.com",
    "batch.googleapis.com",
    "compute.googleapis.com",
    "iam.googleapis.com",
    "logging.googleapis.com",
    "run.googleapis.com",
  ])

  # The package names executors push under.  The cleanup policies below depend on them.
  job_image_package       = "run"
  toolchain_image_package = "toolchain"

  runner_project_roles = toset([
    # Lets the Batch agent on a job machine report the job's state.
    "roles/batch.agentReporter",
    # Lets the job's output reach Cloud Logging, from which the executor relays it.
    "roles/logging.logWriter",
  ])

  submitter_project_roles = toset([
    "roles/batch.jobsEditor",
    "roles/logging.viewer",
    "roles/run.developer",
  ])
}

resource "google_project" "overflow" {
  project_id      = var.project_id
  name            = var.project_name
  billing_account = var.billing_account
  org_id          = var.org_id
  folder_id       = var.folder_id

  # The project holds an operator's images and job history; removing it is never a side
  # effect of removing this module.
  deletion_policy = "PREVENT"
}

resource "google_project_service" "enabled" {
  for_each = local.services

  project            = google_project.overflow.project_id
  service            = each.value
  disable_on_destroy = false
}

resource "google_artifact_registry_repository" "images" {
  project       = google_project.overflow.project_id
  location      = var.region
  repository_id = var.repository_id
  format        = "DOCKER"
  description   = "Toolchain and job images for AGCoord overflow runs"

  # An executor deletes a job's image when the job ends.  These policies are the backstop
  # for an executor that was killed first, and for toolchains nothing uses any more.
  cleanup_policy_dry_run = false

  cleanup_policies {
    id     = "delete-leftover-job-images"
    action = "DELETE"

    condition {
      package_name_prefixes = [local.job_image_package]
      older_than            = "${var.job_image_retention_days * 86400}s"
    }
  }

  cleanup_policies {
    id     = "delete-unused-toolchain-images"
    action = "DELETE"

    condition {
      package_name_prefixes = [local.toolchain_image_package]
      older_than            = "${var.toolchain_image_retention_days * 86400}s"
    }
  }

  depends_on = [google_project_service.enabled]
}

# The identity of a job machine.  It can report to Batch, write logs, and read this one
# repository; it holds no forge credentials and cannot change the project.
resource "google_service_account" "runner" {
  project      = google_project.overflow.project_id
  account_id   = "agcoord-overflow-runner"
  display_name = "AGCoord overflow job runner"

  depends_on = [google_project_service.enabled]
}

resource "google_project_iam_member" "runner" {
  for_each = local.runner_project_roles

  project = google_project.overflow.project_id
  role    = each.value
  member  = google_service_account.runner.member
}

resource "google_artifact_registry_repository_iam_member" "runner_reads_images" {
  project    = google_artifact_registry_repository.images.project
  location   = google_artifact_registry_repository.images.location
  repository = google_artifact_registry_repository.images.name
  role       = "roles/artifactregistry.reader"
  member     = google_service_account.runner.member
}

# A project owner needs none of the grants below.  They let other operators push job
# images, start jobs as the runner, and read the job's output.
resource "google_project_iam_member" "submitter" {
  for_each = {
    for pair in setproduct(var.submitters, local.submitter_project_roles) :
    "${pair[0]} ${pair[1]}" => { member = pair[0], role = pair[1] }
  }

  project = google_project.overflow.project_id
  role    = each.value.role
  member  = each.value.member
}

resource "google_artifact_registry_repository_iam_member" "submitter_manages_images" {
  for_each = var.submitters

  project    = google_artifact_registry_repository.images.project
  location   = google_artifact_registry_repository.images.location
  repository = google_artifact_registry_repository.images.name
  role       = "roles/artifactregistry.repoAdmin"
  member     = each.value
}

resource "google_service_account_iam_member" "submitter_runs_as_runner" {
  for_each = var.submitters

  service_account_id = google_service_account.runner.name
  role               = "roles/iam.serviceAccountUser"
  member             = each.value
}
