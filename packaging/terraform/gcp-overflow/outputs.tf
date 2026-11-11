output "project_id" {
  description = "Project that runs overflow jobs."
  value       = google_project.overflow.project_id
}

output "region" {
  description = "Region of the image repository and of the overflow jobs."
  value       = var.region
}

output "registry" {
  description = "Image path prefix of the repository; executors push PREFIX/toolchain and PREFIX/run."
  value       = "${var.region}-docker.pkg.dev/${google_project.overflow.project_id}/${google_artifact_registry_repository.images.repository_id}"
}

output "job_image_package" {
  description = "Package name executors must push job images under, which the cleanup policy matches."
  value       = local.job_image_package
}

output "toolchain_image_package" {
  description = "Package name executors must push toolchain images under, which the cleanup policy matches."
  value       = local.toolchain_image_package
}

output "runner_service_account" {
  description = "Email of the service account job machines run as."
  value       = google_service_account.runner.email
}
