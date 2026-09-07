output "master_ip" {
  description = "Fixed IPv6 address of the master node"
  value       = openstack_compute_instance_v2.master.network[0].fixed_ip_v6
}

output "worker_ips" {
  description = "Fixed IPv6 addresses of all worker nodes"
  value       = openstack_compute_instance_v2.worker[*].network[0].fixed_ip_v6
}

output "ansible_inventory_path" {
  description = "Path to the generated Ansible inventory file"
  value       = local_file.ansible_inventory.filename
}
