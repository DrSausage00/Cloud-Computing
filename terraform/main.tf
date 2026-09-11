data "openstack_images_image_v2" "ubuntu" {
  name        = var.image_name
  most_recent = true
}

# ── Master Node ──────────────────────────────────────────────────────────────

resource "openstack_compute_instance_v2" "master" {
  name            = "mes-master"
  image_id        = data.openstack_images_image_v2.ubuntu.id
  flavor_name     = var.flavor_master
  key_pair        = var.key_pair
  security_groups = ["default"]

  network {
    name = var.network_name
  }
}

# ── Worker Nodes (×2) ────────────────────────────────────────────────────────

resource "openstack_compute_instance_v2" "worker" {
  count = 2

  name            = "mes-worker-${count.index + 1}"
  image_id        = data.openstack_images_image_v2.ubuntu.id
  flavor_name     = var.flavor_worker
  key_pair        = var.key_pair
  security_groups = ["default"]

  network {
    name = var.network_name
  }
}

# ── Ansible Inventory ────────────────────────────────────────────────────────

resource "local_file" "ansible_inventory" {
  filename = "${path.module}/generated-inventory.yml"
  content = yamlencode({
    all = {
      children = {
        gridflex = {
          children = {
            gridflex_k3s_server = {
              hosts = {
                (openstack_compute_instance_v2.master.network[0].fixed_ip_v6) = {
                  interpreter_python = "/usr/bin/python3"
                  ansible_user       = "ubuntu"
                  ip_family          = "dual"
                  k3s_role           = "server"
                }
              }
            }
            gridflex_k3s_agent = {
              vars = {
                interpreter_python = "/usr/bin/python3"
                k3s_server_host    = openstack_compute_instance_v2.master.network[0].fixed_ip_v6
                k3s_role           = "agent"
                ip_family          = "dual"
              }
              hosts = {
                for w in openstack_compute_instance_v2.worker :
                w.network[0].fixed_ip_v6 => {
                  ansible_user = "ubuntu"
                }
              }
            }
          }
        }
      }
    }
  })
}