variable "project_id" {
  description = "OpenStack Project ID (find at API Access → View Credentials)"
  # default = "aabda742a22344368c56327b0c395909" # stack
  default = "582fd1f55e5246a59d6ba404b4dc9fcd" # ost
}

variable "image_name" {
  description = "ID of the VM image to use"
  # Ubuntu Server 26.04
  # default = "7c6e4b52-4d1f-4da9-b98e-23b17e6570a5"
  default = "Ubuntu 24.04" # ost, Ubuntu 24.04
}

variable "flavor_master" {
  description = "OpenStack flavor (VM size) to use for all nodes"
  default     = "general.medium"
}

variable "flavor_worker" {
  # general.small = 2 vCPU / 8 GB / 50 GB
  default = "general.small"
}

variable "key_pair" {
  description = "Name of the SSH key pair registered in OpenStack"
  default     = "lars-mes"
}

variable "network_name" {
  description = "Name of the OpenStack network to attach VMs to"
  # default = "DHBW"
  default = "DHBWV6"
}

provider "openstack" {
  cloud  = "openstack"
  region = "DHBW-MA"
}