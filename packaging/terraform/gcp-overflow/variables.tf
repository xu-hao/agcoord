variable "project_id" {
  description = "Globally unique, permanent ID of the project that runs overflow jobs."
  type        = string
}

variable "project_name" {
  description = "Display name of the project."
  type        = string
  default     = "AGCoord overflow"
}

variable "billing_account" {
  description = "Billing account ID to attach, as XXXXXX-XXXXXX-XXXXXX. Keep it in an uncommitted variable file."
  type        = string
}

variable "org_id" {
  description = "Organization that owns the project, or null for a project without one."
  type        = string
  default     = null
}

variable "folder_id" {
  description = "Folder that holds the project, or null. Set at most one of org_id and folder_id."
  type        = string
  default     = null
}

variable "region" {
  description = "Region of the image repository and of the overflow jobs."
  type        = string
  default     = "us-central1"
}

variable "repository_id" {
  description = "Name of the Artifact Registry Docker repository that holds toolchain and job images."
  type        = string
  default     = "agcoord-overflow"
}

variable "job_image_retention_days" {
  description = "Days after which a job image the executor failed to delete is removed."
  type        = number
  default     = 2

  validation {
    condition     = var.job_image_retention_days >= 1
    error_message = "job_image_retention_days must be at least 1."
  }
}

variable "toolchain_image_retention_days" {
  description = "Days after which a toolchain image is removed. An executor pushes it again when it is next needed."
  type        = number
  default     = 30

  validation {
    condition     = var.toolchain_image_retention_days >= 1
    error_message = "toolchain_image_retention_days must be at least 1."
  }
}

variable "submitters" {
  description = "IAM members, such as user:alice@example.com, allowed to run overflow jobs without being project owners."
  type        = set(string)
  default     = []
}
